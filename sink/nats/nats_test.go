package nats

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/johndoe/nats-scraper/sink"
	server "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
)

func TestPrepareAndPublishMatchCoreNATSAdapter(t *testing.T) {
	nc := runCoreNATSServer(t)
	subscription, err := nc.SubscribeSync("scrape.>")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Publish(ctx, slog.Default(), nc, "scrape.123.start", 123); err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{
		[]byte(`{"server":{"id":"A"},"data":{}}`),
		[]byte(`{"server":{"id":"B"},"data":{}}`),
	}
	count, err := Prepare(ctx, slog.Default(), nc, "scrape.123.varz", 123, payloads, 0)
	if err != nil || count != 2 {
		t.Fatalf("Prepare = (%d, %v), want (2, nil)", count, err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	marker, err := subscription.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if marker.Subject != "scrape.123.start" || marker.Header.Get(sink.EpochHeader) != "123" || len(marker.Data) != 0 {
		t.Fatalf("marker = subject %q headers %#v data %q", marker.Subject, marker.Header, marker.Data)
	}
	decompressor := sink.NewDecompressor()
	buffer := &bytes.Buffer{}
	for _, serverID := range []string{"A", "B"} {
		message, err := subscription.NextMsg(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		value, endpoint, epoch, err := sink.Parse(decompressor, buffer, message.Subject, message.Data, message.Header)
		if err != nil {
			t.Fatal(err)
		}
		if value == nil || endpoint != "varz" || epoch != 123 || message.Header.Get(sink.ServerHeader) != serverID {
			t.Fatalf("data message = %T endpoint %q epoch %d headers %#v", value, endpoint, epoch, message.Header)
		}
	}
}

func TestCoreNATSPublishErrorsAreReturned(t *testing.T) {
	nc := runCoreNATSServer(t)
	nc.Close()

	if err := Publish(context.Background(), slog.Default(), nc, "scrape.1.start", 1); !errors.Is(err, natsgo.ErrConnectionClosed) {
		t.Fatalf("Publish error = %v, want %v", err, natsgo.ErrConnectionClosed)
	}
	count, err := Prepare(
		context.Background(),
		slog.Default(),
		nc,
		"scrape.1.varz",
		1,
		[][]byte{[]byte(`{"server":{"id":"A"}}`)},
		1,
	)
	if count != 0 || !errors.Is(err, natsgo.ErrConnectionClosed) {
		t.Fatalf("Prepare = (%d, %v), want (0, %v)", count, err, natsgo.ErrConnectionClosed)
	}
}

func runCoreNATSServer(t *testing.T) *natsgo.Conn {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host:            "127.0.0.1",
		Port:            -1,
		NoLog:           true,
		NoSigs:          true,
		NoSystemAccount: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := natsgo.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}
