package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	scraper "github.com/johndoe/nats-scraper"
	"github.com/johndoe/nats-scraper/runner"
	"github.com/johndoe/nats-scraper/sink"
	jssink "github.com/johndoe/nats-scraper/sink/jetstream"
	"github.com/nats-io/nats.go"
)

type observation struct {
	Index           int    `json:"index"`
	Subject         string `json:"subject"`
	Endpoint        string `json:"endpoint"`
	Epoch           int64  `json:"epoch"`
	Server          string `json:"server,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	PayloadBytes    int    `json:"payload_bytes"`
	PayloadSHA256   string `json:"payload_sha256"`
}

type manifest struct {
	mu        sync.Mutex
	encoder   *json.Encoder
	index     int
	completed bool
}

func main() {
	serverURL := flag.String("server", nats.DefaultURL, "NATS system server URL")
	creds := flag.String("creds", "", "optional NATS credentials file")
	user := flag.String("user", "", "optional NATS username")
	password := flag.String("password", "", "optional NATS password")
	prefix := flag.String("prefix", "reconstructed", "manifest subject prefix")
	sinkServer := flag.String("sink-server", "", "optional JetStream sink server URL")
	sinkCreds := flag.String("sink-creds", "", "optional JetStream sink credentials file")
	sinkStream := flag.String("sink-stream", "reconstructed", "precreated JetStream sink stream")
	denyServerNames := flag.String("deny-server-names", "", "comma-separated discovered server names to exclude")
	disabledEndpoints := flag.String("disabled-endpoints", "", "comma-separated endpoint names to disable")
	endpointFrequencies := flag.String("endpoint-frequencies", "", "comma-separated endpoint=frequency overrides")
	scrapeTimeout := flag.Duration("scrape-timeout", 30*time.Second, "scraper request timeout")
	maxRuntime := flag.Duration("max-runtime", 2*time.Minute, "maximum time for one completed scrape")
	flag.Parse()

	if err := scrapeOnce(*serverURL, *creds, *user, *password, *prefix, *sinkServer, *sinkCreds, *sinkStream, *denyServerNames, *disabledEndpoints, *endpointFrequencies, *scrapeTimeout, *maxRuntime); err != nil {
		fmt.Fprintln(os.Stderr, "one-shot scrape failed:", err)
		os.Exit(1)
	}
}

func scrapeOnce(serverURL, creds, user, password, prefix, sinkServer, sinkCreds, sinkStream, denyServerNames, disabledEndpoints, endpointFrequencies string, scrapeTimeout, maxRuntime time.Duration) error {
	options := []nats.Option{nats.Name("reconstructed-scraper-once")}
	if creds != "" {
		options = append(options, nats.UserCredentials(creds))
	}
	if user != "" || password != "" {
		options = append(options, nats.UserInfo(user, password))
	}
	nc, err := nats.Connect(serverURL, options...)
	if err != nil {
		return err
	}
	defer nc.Close()

	sinkConnection := nc
	if sinkServer != "" {
		sinkOptions := []nats.Option{nats.Name("reconstructed-scraper-sink")}
		if sinkCreds != "" {
			sinkOptions = append(sinkOptions, nats.UserCredentials(sinkCreds))
		}
		sinkConnection, err = nats.Connect(sinkServer, sinkOptions...)
		if err != nil {
			return err
		}
		defer sinkConnection.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), maxRuntime)
	defer cancel()
	output := &manifest{encoder: json.NewEncoder(os.Stdout)}
	config := runner.DefaultConfig
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	config.SubjectPrefix = prefix
	config.ScrapeTimeout = scrapeTimeout
	config.SinkType = "nats"
	config.OnDemandDisabled = true
	if err := applyEndpointOverrides(&config, disabledEndpoints, endpointFrequencies); err != nil {
		return err
	}
	if sinkServer != "" {
		config.SinkType = "jetstream"
		config.StreamName = sinkStream
		config.SinkPrepare = jssink.Prepare
		config.SinkPublish = func(
			ctx context.Context,
			logger *slog.Logger,
			nc *nats.Conn,
			subject string,
			epoch int64,
		) error {
			err := jssink.Publish(ctx, logger, nc, subject, epoch)
			if err == nil && strings.HasSuffix(subject, ".end") {
				output.mu.Lock()
				output.completed = true
				output.mu.Unlock()
				cancel()
			}
			return err
		}
	} else {
		config.SinkPrepare = output.prepare
		config.SinkPublish = func(
			_ context.Context,
			_ *slog.Logger,
			_ *nats.Conn,
			subject string,
			epoch int64,
		) error {
			err := output.publishMarker(subject, epoch)
			if err == nil && strings.HasSuffix(subject, ".end") {
				output.mu.Lock()
				output.completed = true
				output.mu.Unlock()
				cancel()
			}
			return err
		}
	}
	deniedNames := splitSet(denyServerNames)
	if len(deniedNames) != 0 {
		config.ServerFilter = func(info scraper.ServerInfo) bool {
			_, denied := deniedNames[info.Name]
			return !denied
		}
	}
	startErr := runner.New(&config, sinkConnection, nc).Start(ctx)
	if startErr != nil {
		return startErr
	}
	output.mu.Lock()
	completed := output.completed
	output.mu.Unlock()
	if !completed {
		if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return errors.New("scrape ended without an end marker")
	}
	return nil
}

func applyEndpointOverrides(config *runner.Config, disabled, frequencies string) error {
	for name := range splitSet(disabled) {
		if !setEndpointDisabled(config, name) {
			return fmt.Errorf("unknown disabled endpoint %q", name)
		}
	}
	for _, item := range strings.Split(frequencies, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("invalid endpoint frequency %q: expected endpoint=positive-integer", item)
		}
		frequency, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || frequency <= 0 {
			return fmt.Errorf("invalid endpoint frequency %q: expected endpoint=positive-integer", item)
		}
		if !setEndpointFrequency(config, strings.TrimSpace(name), frequency) {
			return fmt.Errorf("unknown frequency endpoint %q", strings.TrimSpace(name))
		}
	}
	return nil
}

func setEndpointDisabled(config *runner.Config, name string) bool {
	switch name {
	case "varz":
		config.Varz.Disabled = true
	case "connz":
		config.Connz.Disabled = true
	case "subsz":
		config.Subsz.Disabled = true
	case "routez":
		config.Routez.Disabled = true
	case "jsz":
		config.Jsz.Disabled = true
	case "gatewayz":
		config.Gatewayz.Disabled = true
	case "leafz":
		config.Leafz.Disabled = true
	case "accountz":
		config.Accountz.Disabled = true
	case "accstatz":
		config.Accstatz.Disabled = true
	case "healthz":
		config.Healthz.Disabled = true
	case "raftz":
		config.Raftz.Disabled = true
	case "ipqueuesz":
		config.Ipqueuesz.Disabled = true
	default:
		return false
	}
	return true
}

func setEndpointFrequency(config *runner.Config, name string, frequency int) bool {
	switch name {
	case "varz":
		config.Varz.Frequency = frequency
	case "connz":
		config.Connz.Frequency = frequency
	case "subsz":
		config.Subsz.Frequency = frequency
	case "routez":
		config.Routez.Frequency = frequency
	case "jsz":
		config.Jsz.Frequency = frequency
	case "gatewayz":
		config.Gatewayz.Frequency = frequency
	case "leafz":
		config.Leafz.Frequency = frequency
	case "accountz":
		config.Accountz.Frequency = frequency
	case "accstatz":
		config.Accstatz.Frequency = frequency
	case "healthz":
		config.Healthz.Frequency = frequency
	case "raftz":
		config.Raftz.Frequency = frequency
	case "ipqueuesz":
		config.Ipqueuesz.Frequency = frequency
	default:
		return false
	}
	return true
}

func splitSet(value string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result[item] = struct{}{}
		}
	}
	return result
}

func (m *manifest) prepare(
	_ context.Context,
	_ *slog.Logger,
	_ *nats.Conn,
	subject string,
	epoch int64,
	payloads [][]byte,
	_ int,
) (int, error) {
	compressor := sink.NewCompressor()
	defer compressor.Close()
	messages, err := sink.PrepareBatch(compressor, subject, epoch, payloads)
	if err != nil {
		return 0, err
	}
	for _, message := range messages {
		if err := m.encode(message, endpointFromSubject(subject), epoch); err != nil {
			return 0, err
		}
	}
	return len(messages), nil
}

func (m *manifest) publishMarker(subject string, epoch int64) error {
	message, err := sink.PreparePublish(subject, epoch)
	if err != nil {
		return err
	}
	return m.encode(message, endpointFromSubject(subject), epoch)
}

func (m *manifest) encode(message *nats.Msg, endpoint string, epoch int64) error {
	digest := sha256.Sum256(message.Data)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.index++
	return m.encoder.Encode(observation{
		Index:           m.index,
		Subject:         message.Subject,
		Endpoint:        endpoint,
		Epoch:           epoch,
		Server:          message.Header.Get(sink.ServerHeader),
		ContentEncoding: message.Header.Get(sink.ContentEncodingHeader),
		PayloadBytes:    len(message.Data),
		PayloadSHA256:   hex.EncodeToString(digest[:]),
	})
}

func endpointFromSubject(subject string) string {
	if index := strings.LastIndexByte(subject, '.'); index >= 0 {
		return subject[index+1:]
	}
	return subject
}
