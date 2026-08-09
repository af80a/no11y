package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/johndoe/nats-scraper/sink"
	jssink "github.com/johndoe/nats-scraper/sink/jetstream"
	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

type observation struct {
	Index           int      `json:"index"`
	Subject         string   `json:"subject"`
	Endpoint        string   `json:"endpoint"`
	Epoch           int64    `json:"epoch"`
	Server          string   `json:"server,omitempty"`
	ContentEncoding string   `json:"content_encoding,omitempty"`
	PayloadBytes    int      `json:"payload_bytes"`
	PayloadSHA256   string   `json:"payload_sha256"`
	DecodedBytes    int      `json:"decoded_bytes,omitempty"`
	TopLevelKeys    []string `json:"top_level_keys,omitempty"`
	ParsedType      string   `json:"parsed_type,omitempty"`
	ReencodedMatch  *bool    `json:"reencoded_match,omitempty"`
}

type streamState struct {
	Stream        string `json:"stream"`
	Messages      uint64 `json:"messages"`
	FirstSequence uint64 `json:"first_sequence"`
	LastSequence  uint64 `json:"last_sequence"`
	LastEpoch     int64  `json:"last_epoch,omitempty"`
}

type observer struct {
	parser        *sink.Decompressor
	buffer        *bytes.Buffer
	encoder       *json.Encoder
	capturedEpoch int64
	index         int
}

func main() {
	serverURL := flag.String("server", nats.DefaultURL, "NATS sink server URL")
	creds := flag.String("creds", "", "optional NATS credentials file")
	prefix := flag.String("prefix", "scrape", "sink subject prefix")
	stored := flag.Bool("stored", false, "replay one persisted epoch from JetStream")
	stateOnly := flag.Bool("state", false, "print JetStream message-count metadata without reading payloads")
	streamName := flag.String("stream", "scrape", "JetStream stream used with -stored or -state")
	timeout := flag.Duration("timeout", 30*time.Second, "maximum time to capture one epoch")
	flag.Parse()

	if err := inspect(*serverURL, *creds, *prefix, *streamName, *stored, *stateOnly, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "sink inspection failed:", err)
		os.Exit(1)
	}
}

func inspect(serverURL, creds, prefix, streamName string, stored, stateOnly bool, timeout time.Duration) error {
	options := []nats.Option{nats.Name("scraper-sink-inspect")}
	if creds != "" {
		options = append(options, nats.UserCredentials(creds))
	}
	nc, err := nats.Connect(serverURL, options...)
	if err != nil {
		return err
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if stateOnly {
		return inspectState(ctx, nc, streamName, prefix)
	}
	observer := &observer{
		parser:  sink.NewDecompressor(),
		buffer:  &bytes.Buffer{},
		encoder: json.NewEncoder(os.Stdout),
	}
	if stored {
		return inspectStored(ctx, nc, streamName, prefix, observer)
	}
	return inspectLive(ctx, nc, prefix, timeout, observer)
}

func inspectState(ctx context.Context, nc *nats.Conn, streamName, prefix string) error {
	js, err := jsapi.New(nc)
	if err != nil {
		return err
	}
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	lastEpoch, err := jssink.LastEpoch(ctx, nc, streamName, prefix)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(streamState{
		Stream:        streamName,
		Messages:      info.State.Msgs,
		FirstSequence: info.State.FirstSeq,
		LastSequence:  info.State.LastSeq,
		LastEpoch:     lastEpoch,
	})
}

func inspectLive(ctx context.Context, nc *nats.Conn, prefix string, timeout time.Duration, observer *observer) error {
	subscription, err := nc.SubscribeSync(prefix + ".>")
	if err != nil {
		return err
	}
	if err := nc.Flush(); err != nil {
		return err
	}
	for {
		message, err := subscription.NextMsgWithContext(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("capture timed out after %s", timeout)
			}
			return err
		}
		done, err := observer.observe(message.Subject, message.Data, message.Header)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func inspectStored(ctx context.Context, nc *nats.Conn, streamName, prefix string, observer *observer) error {
	js, err := jsapi.New(nc)
	if err != nil {
		return err
	}
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}
	consumer, err := stream.OrderedConsumer(ctx, jsapi.OrderedConsumerConfig{
		FilterSubjects: []string{prefix + ".>"},
		DeliverPolicy:  jsapi.DeliverAllPolicy,
	})
	if err != nil {
		return err
	}
	messages, err := consumer.Messages()
	if err != nil {
		return err
	}
	defer messages.Stop()

	for {
		message, err := messages.Next(jsapi.NextContext(ctx))
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("stored capture timed out: %w", err)
			}
			return err
		}
		done, err := observer.observe(message.Subject(), message.Data(), message.Headers())
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (o *observer) observe(subject string, data []byte, headers nats.Header) (bool, error) {
	value, endpoint, epoch, err := sink.Parse(o.parser, o.buffer, subject, data, headers)
	if err != nil {
		return false, fmt.Errorf("parse %q: %w", subject, err)
	}
	if o.capturedEpoch == 0 {
		if endpoint != "start" {
			return false, nil
		}
		o.capturedEpoch = epoch
	}
	if epoch != o.capturedEpoch {
		return false, nil
	}

	digest := sha256.Sum256(data)
	parsedType := ""
	decodedBytes := 0
	var topLevelKeys []string
	var reencodedMatch *bool
	if value != nil {
		parsedType = fmt.Sprintf("%T", value)
		decodedBytes = o.buffer.Len()
		var object map[string]json.RawMessage
		if json.Unmarshal(o.buffer.Bytes(), &object) == nil {
			for key := range object {
				topLevelKeys = append(topLevelKeys, key)
			}
			sort.Strings(topLevelKeys)
		}
		compressor := sink.NewCompressor()
		prepared, prepareErr := sink.PrepareBatch(
			compressor,
			subject,
			epoch,
			[][]byte{o.buffer.Bytes()},
		)
		closeErr := compressor.Close()
		if prepareErr != nil {
			return false, fmt.Errorf("re-encode %q: %w", subject, prepareErr)
		}
		if closeErr != nil {
			return false, fmt.Errorf("close re-encoder for %q: %w", subject, closeErr)
		}
		matches := len(prepared) == 1 &&
			bytes.Equal(prepared[0].Data, data) &&
			reflect.DeepEqual(prepared[0].Header, headers)
		reencodedMatch = &matches
	}
	o.index++
	if err := o.encoder.Encode(observation{
		Index:           o.index,
		Subject:         subject,
		Endpoint:        endpoint,
		Epoch:           epoch,
		Server:          headers.Get(sink.ServerHeader),
		ContentEncoding: headers.Get(sink.ContentEncodingHeader),
		PayloadBytes:    len(data),
		PayloadSHA256:   hex.EncodeToString(digest[:]),
		DecodedBytes:    decodedBytes,
		TopLevelKeys:    topLevelKeys,
		ParsedType:      parsedType,
		ReencodedMatch:  reencodedMatch,
	}); err != nil {
		return false, err
	}
	return endpoint == "end" && strings.HasSuffix(subject, ".end"), nil
}
