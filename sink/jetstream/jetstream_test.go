package jetstream

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

var _ func(context.Context, *slog.Logger, jsapi.JetStream, []*nats.Msg, int) (int, error) = publishBatch

func TestParserLayoutMatchesBinary(t *testing.T) {
	var parser Parser
	if got := unsafe.Sizeof(parser); got != 0x10 {
		t.Fatalf("Parser size = %#x, want %#x", got, uintptr(0x10))
	}
	if got := unsafe.Offsetof(parser.dec); got != 0 {
		t.Fatalf("dec offset = %#x, want 0", got)
	}
	if got := unsafe.Offsetof(parser.buf); got != 8 {
		t.Fatalf("buf offset = %#x, want 8", got)
	}
	typeOf := reflect.TypeOf(parser)
	for i, want := range []string{"dec", "buf"} {
		if got := typeOf.Field(i).Name; got != want {
			t.Errorf("field %d name = %q, want %q", i, got, want)
		}
	}
}

func TestJetStreamPublishLastEpochAndPrune(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	baseSubscriptions := nc.NumSubscriptions()

	payload := func(id string) []byte {
		return []byte(`{"server":{"id":"` + id + `"},"data":{}}`)
	}
	for _, epoch := range []int64{100, 200} {
		if err := Publish(ctx, slog.Default(), nc, "scrape."+formatEpoch(epoch)+".start", epoch); err != nil {
			t.Fatal(err)
		}
		if count, err := Prepare(ctx, slog.Default(), nc, "scrape."+formatEpoch(epoch)+".varz", epoch, [][]byte{payload("S1"), payload("S2")}, 1); err != nil || count != 2 {
			t.Fatalf("prepare count/error = %d/%v", count, err)
		}
		if err := Publish(ctx, slog.Default(), nc, "scrape."+formatEpoch(epoch)+".end", epoch); err != nil {
			t.Fatal(err)
		}
	}
	if got := nc.NumSubscriptions(); got != baseSubscriptions {
		t.Fatalf("subscriptions after prepare = %d, want %d", got, baseSubscriptions)
	}

	last, err := LastEpoch(ctx, nc, "scrape", "scrape")
	if err != nil {
		t.Fatal(err)
	}
	if last != 200 {
		t.Fatalf("last epoch = %d, want 200", last)
	}
	pruned, err := Prune(ctx, nc, "scrape", "scrape", 1)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Fatalf("pruned epochs = %d, want 1", pruned)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("messages after prune = %d, want the newest end marker only", info.State.Msgs)
	}
}

func TestParserParsesJetStreamMessage(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"server":{"id":"S1"},"data":{}}`)
	if count, err := Prepare(ctx, slog.Default(), nc, "scrape.123.varz", 123, [][]byte{payload}, 1); err != nil || count != 1 {
		t.Fatalf("prepare count/error = %d/%v", count, err)
	}
	consumer, err := stream.OrderedConsumer(ctx, jsapi.OrderedConsumerConfig{
		FilterSubjects: []string{"scrape.123.varz"},
		DeliverPolicy:  jsapi.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(1, jsapi.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for msg := range batch.Messages() {
		value, endpoint, epoch, err := NewParser().Parse(msg)
		if err != nil {
			t.Fatal(err)
		}
		response, ok := value.(*server.ServerAPIVarzResponse)
		if !ok || response.Server == nil || response.Server.ID != "S1" {
			t.Fatalf("parsed response = %#v", value)
		}
		if endpoint != "varz" || epoch != 123 {
			t.Fatalf("endpoint/epoch = %q/%d", endpoint, epoch)
		}
		return
	}
	if err := batch.Error(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("consumer returned no message")
}

func TestLastEpochEmptyStream(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}

	epoch, err := LastEpoch(ctx, nc, "scrape", "scrape")
	if err != nil || epoch != 0 {
		t.Fatalf("last epoch = %d, error = %v; want 0, nil", epoch, err)
	}
}

func TestLastEpochMissingHeader(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, "scrape.100.end", nil); err != nil {
		t.Fatal(err)
	}

	epoch, err := LastEpoch(ctx, nc, "scrape", "scrape")
	if err != nil || epoch != 0 {
		t.Fatalf("last epoch = %d, error = %v; want 0, nil", epoch, err)
	}
}

func TestPruneDeduplicatesAdjacentEpochHeaders(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range []int64{100, 100, 200} {
		if err := Publish(ctx, slog.Default(), nc, "scrape."+formatEpoch(epoch)+".end", epoch); err != nil {
			t.Fatal(err)
		}
	}

	pruned, err := Prune(ctx, nc, "scrape", "scrape", 2)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 0 {
		t.Fatalf("pruned epochs = %d, want 0", pruned)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 3 {
		t.Fatalf("messages after prune = %d, want 3", info.State.Msgs)
	}
}

func TestPublishBatchLogsAsyncNegativeAcknowledgementButReturnsSuccess(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:       "limited",
		Subjects:   []string{"limited.>"},
		Storage:    jsapi.MemoryStorage,
		MaxMsgSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	message := nats.NewMsg("limited.data")
	message.Data = []byte("too large")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	count, err := publishBatch(ctx, logger, js, []*nats.Msg{message}, 1)
	if err != nil || count != 1 {
		t.Fatalf("publishBatch count/error = %d/%v, want 1/nil", count, err)
	}
	if !bytes.Contains(logs.Bytes(), []byte("stream/publish")) ||
		!bytes.Contains(logs.Bytes(), []byte("subject=limited.data")) {
		t.Fatalf("negative acknowledgement log = %q", logs.String())
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("stored messages = %d, want rejected publish absent", info.State.Msgs)
	}
}

func TestPublishReturnsSynchronousNegativeAcknowledgement(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:       "limited-marker",
		Subjects:   []string{"limited-marker.>"},
		Storage:    jsapi.MemoryStorage,
		MaxMsgSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = Publish(ctx, slog.Default(), nc, "limited-marker.1.start", 1)
	if err == nil || !strings.Contains(err.Error(), "err_code=10054") {
		t.Fatalf("Publish error = %v, want JetStream maximum-message-size error", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("stored messages = %d, want rejected marker absent", info.State.Msgs)
	}
}

func TestPublishBatchImmediateErrorReturnsZeroAfterPartialSubmission(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "partial",
		Subjects: []string{"partial.>"},
		Storage:  jsapi.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	valid := nats.NewMsg("partial.data")
	invalid := nats.NewMsg("")
	count, err := publishBatch(ctx, slog.Default(), js, []*nats.Msg{valid, invalid}, 10)
	if count != 0 || !errors.Is(err, nats.ErrBadSubject) {
		t.Fatalf("publishBatch = (%d, %v), want (0, %v)", count, err, nats.ErrBadSubject)
	}
	<-js.PublishAsyncComplete()
	js.CleanupPublisher()
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stored messages = %d, want first submission retained", info.State.Msgs)
	}
}

func TestPruneTruncatesOldestOfMultipleRetainedEpochs(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, epoch := range []int64{100, 200, 300} {
		if err := Publish(ctx, slog.Default(), nc, "scrape."+formatEpoch(epoch)+".start", epoch); err != nil {
			t.Fatal(err)
		}
		if count, err := Prepare(ctx, slog.Default(), nc, "scrape."+formatEpoch(epoch)+".varz", epoch, [][]byte{[]byte(`{"server":{"id":"S1"},"data":{}}`)}, 1); err != nil || count != 1 {
			t.Fatalf("prepare count/error = %d/%v", count, err)
		}
		if err := Publish(ctx, slog.Default(), nc, "scrape."+formatEpoch(epoch)+".end", epoch); err != nil {
			t.Fatal(err)
		}
	}

	pruned, err := Prune(ctx, nc, "scrape", "scrape", 2)
	if err != nil || pruned != 1 {
		t.Fatalf("Prune count/error = %d/%v, want 1/nil", pruned, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 4 {
		t.Fatalf("messages after prune = %d, want epoch 200 end plus complete epoch 300", info.State.Msgs)
	}
	if info.State.FirstSeq != 6 {
		t.Fatalf("first sequence after prune = %d, want epoch 200 end sequence 6", info.State.FirstSeq)
	}
}

func TestPruneNonPositiveMaximumIsNoopWithoutConnection(t *testing.T) {
	for _, maximum := range []int{0, -1} {
		pruned, err := Prune(context.Background(), nil, "missing", "scrape", maximum)
		if err != nil || pruned != 0 {
			t.Fatalf("Prune maximum %d = %d/%v, want 0/nil", maximum, pruned, err)
		}
	}
}

func TestPruneCountsInteriorMissingEpochHeader(t *testing.T) {
	nc := runJetStreamServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(ctx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, slog.Default(), nc, "scrape.100.end", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, "scrape.missing.end", nil); err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, slog.Default(), nc, "scrape.200.end", 200); err != nil {
		t.Fatal(err)
	}

	pruned, err := Prune(ctx, nc, "scrape", "scrape", 2)
	if err != nil || pruned != 1 {
		t.Fatalf("Prune = (%d, %v), want (1, nil)", pruned, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 || info.State.FirstSeq != 2 {
		t.Fatalf("state = messages %d first %d, want 2/2", info.State.Msgs, info.State.FirstSeq)
	}
}

func runJetStreamServer(t *testing.T) *nats.Conn {
	t.Helper()
	options := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := server.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func formatEpoch(epoch int64) string {
	return strconv.FormatInt(epoch, 10)
}
