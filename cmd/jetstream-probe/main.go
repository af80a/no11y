package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strings"
	"time"

	jssink "github.com/johndoe/nats-scraper/sink/jetstream"
	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

type observation struct {
	Stream             string `json:"stream"`
	Created            bool   `json:"created,omitempty"`
	PreviousMaxMsgSize int32  `json:"previous_max_msg_size"`
	CurrentMaxMsgSize  int32  `json:"current_max_msg_size"`
	MessagesBefore     uint64 `json:"messages_before"`
	MessagesAfter      uint64 `json:"messages_after"`
	ProbeSubject       string `json:"probe_subject,omitempty"`
	MarkerSubject      string `json:"marker_subject,omitempty"`
	MarkerError        string `json:"marker_error,omitempty"`
	Submitted          int    `json:"submitted,omitempty"`
	PrepareCount       int    `json:"prepare_count,omitempty"`
	PrepareError       string `json:"prepare_error,omitempty"`
	Logs               string `json:"logs,omitempty"`
}

func main() {
	serverURL := flag.String("server", nats.DefaultURL, "NATS JetStream server URL")
	creds := flag.String("creds", "", "optional NATS credentials file")
	user := flag.String("user", "", "optional NATS username")
	password := flag.String("password", "", "optional NATS password")
	streamName := flag.String("stream", "scrape", "JetStream stream name")
	create := flag.Bool("create", false, "create the stream if it does not exist")
	streamSubject := flag.String("stream-subject", "scrape.>", "subject used when creating the stream")
	maxMsgSize := flag.Int64("max-msg-size", math.MinInt32, "new maximum message size; omit to preserve the current value")
	probeSubject := flag.String("probe-subject", "", "optional subject to publish through the reconstructed sink adapter")
	markerSubject := flag.String("marker-subject", "", "optional marker subject to publish synchronously")
	probeCount := flag.Int("probe-count", 3, "number of synthetic payloads to prepare")
	padding := flag.Int("padding", 256, "bytes of repetitive padding in each synthetic payload")
	epoch := flag.Int64("epoch", 1, "epoch used for synthetic payload headers")
	batchSize := flag.Int("batch-size", 1, "async publish batch size")
	timeout := flag.Duration("timeout", 30*time.Second, "operation timeout")
	flag.Parse()

	if err := probe(*serverURL, *creds, *user, *password, *streamName, *create, *streamSubject, *maxMsgSize, *probeSubject, *markerSubject, *probeCount, *padding, *epoch, *batchSize, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "JetStream probe failed:", err)
		os.Exit(1)
	}
}

func probe(serverURL, creds, user, password, streamName string, create bool, streamSubject string, maxMsgSize int64, probeSubject, markerSubject string, probeCount, padding int, epoch int64, batchSize int, timeout time.Duration) error {
	if maxMsgSize < math.MinInt32 || maxMsgSize > math.MaxInt32 {
		return fmt.Errorf("max message size %d does not fit int32", maxMsgSize)
	}
	if probeCount < 0 {
		return fmt.Errorf("probe count must not be negative")
	}
	if padding < 0 {
		return fmt.Errorf("padding must not be negative")
	}

	options := []nats.Option{nats.Name("scraper-jetstream-probe")}
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

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		return err
	}
	stream, err := js.Stream(ctx, streamName)
	created := false
	if err != nil && create {
		stream, err = js.CreateStream(ctx, jsapi.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubject},
			Storage:  jsapi.FileStorage,
		})
		created = err == nil
	}
	if err != nil {
		return err
	}
	before, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	previousMaxMsgSize := before.Config.MaxMsgSize
	if maxMsgSize != math.MinInt32 {
		config := before.Config
		config.MaxMsgSize = int32(maxMsgSize)
		stream, err = js.UpdateStream(ctx, config)
		if err != nil {
			return err
		}
	}

	result := observation{
		Stream:             streamName,
		Created:            created,
		PreviousMaxMsgSize: previousMaxMsgSize,
		CurrentMaxMsgSize:  stream.CachedInfo().Config.MaxMsgSize,
		MessagesBefore:     before.State.Msgs,
		ProbeSubject:       probeSubject,
		MarkerSubject:      markerSubject,
	}
	if markerSubject != "" {
		if err := jssink.Publish(ctx, slog.Default(), nc, markerSubject, epoch); err != nil {
			result.MarkerError = err.Error()
		}
	}
	if probeSubject != "" {
		payloads := make([][]byte, probeCount)
		for index := range payloads {
			payloads[index] = []byte(fmt.Sprintf(
				`{"server":{"id":"PROBE-%d"},"data":{"padding":"%s"}}`,
				index+1,
				strings.Repeat("x", padding),
			))
		}
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		result.Submitted = len(payloads)
		result.PrepareCount, err = jssink.Prepare(ctx, logger, nc, probeSubject, epoch, payloads, batchSize)
		if err != nil {
			result.PrepareError = err.Error()
		}
		result.Logs = logs.String()
	}
	after, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	result.CurrentMaxMsgSize = after.Config.MaxMsgSize
	result.MessagesAfter = after.State.Msgs
	return json.NewEncoder(os.Stdout).Encode(result)
}
