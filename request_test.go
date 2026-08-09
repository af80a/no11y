package scraper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/klauspost/compress/s2"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var (
	_ func(context.Context, any, string, int, *nats.Conn, time.Duration, *slog.Logger) ([][]byte, error)   = doReq
	_ func(context.Context, any, string, int, *nats.Conn, time.Duration, *slog.Logger, func([]byte)) error = doReqAsync
)

func TestRequestorLayoutMatchesBinary(t *testing.T) {
	var requestor requestor
	for name, gotWant := range map[string][2]uintptr{
		"ids":      {unsafe.Offsetof(requestor.ids), 0x00},
		"nc":       {unsafe.Offsetof(requestor.nc), 0x18},
		"timeout":  {unsafe.Offsetof(requestor.timeout), 0x20},
		"logger":   {unsafe.Offsetof(requestor.logger), 0x28},
		"targeted": {unsafe.Offsetof(requestor.targeted), 0x30},
		"included": {unsafe.Offsetof(requestor.included), 0x38},
	} {
		if gotWant[0] != gotWant[1] {
			t.Errorf("%s offset = %#x, want %#x", name, gotWant[0], gotWant[1])
		}
	}
	if got := unsafe.Sizeof(requestor); got != 0x40 {
		t.Fatalf("requestor size = %#x, want %#x", got, uintptr(0x40))
	}
}

func TestRequestCompressionSubjectRules(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	tests := []struct {
		subject string
		want    string
	}{
		{subject: ServerPingSubject},
		{subject: AccountStatzSubject},
		{subject: "$SYS.REQ.ACCOUNT.A.STATZ"},
		{subject: VarzSubject, want: "snappy"},
		{subject: "$SYS.REQ.SERVER.S1.ACCOUNTZ", want: "snappy"},
		{subject: "ordinary", want: "snappy"},
	}
	for _, test := range tests {
		seen := make(chan string, 1)
		subscription, err := nc.Subscribe(test.subject, func(request *nats.Msg) {
			seen <- request.Header.Get("Accept-Encoding")
			_ = request.Respond([]byte(`{}`))
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		_, err = doReq(context.Background(), nil, test.subject, 1, nc, time.Second, slog.Default())
		if err != nil {
			t.Fatalf("doReq(%q): %v", test.subject, err)
		}
		if got := <-seen; got != test.want {
			t.Errorf("Accept-Encoding for %q = %q, want %q", test.subject, got, test.want)
		}
		if err := subscription.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRequestNegotiatesAndDecodesSnappy(t *testing.T) {
	_, nc := runTestNATSServer(t, false)

	sub, err := nc.Subscribe("monitor", func(request *nats.Msg) {
		if got := request.Header.Get("Accept-Encoding"); got != "snappy" {
			t.Errorf("Accept-Encoding = %q, want snappy", got)
		}
		var options map[string]any
		if err := json.Unmarshal(request.Data, &options); err != nil {
			t.Errorf("invalid request JSON: %v", err)
		}
		response := []byte(`{"server":{"id":"S1"},"data":{"ok":true}}`)
		msg := nats.NewMsg(request.Reply)
		msg.Header.Set("Content-Encoding", "snappy")
		msg.Data = snappyFrame(t, response)
		if err := nc.PublishMsg(msg); err != nil {
			t.Errorf("publish response: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	requestor := NewRequestor([]string{"S1"}, nc, time.Second, slog.Default(), 0)
	responses, err := requestor.Request(context.Background(), "monitor", map[string]bool{"details": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || !bytes.Contains(responses[0], []byte(`"ok":true`)) {
		t.Fatalf("unexpected responses: %q", responses)
	}
}

func TestTargetedRequestUsesDirectServerSubjects(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	seen := make(chan string, 2)
	sub, err := nc.Subscribe("$SYS.REQ.SERVER.*.VARZ", func(request *nats.Msg) {
		tokens := strings.Split(request.Subject, ".")
		serverID := tokens[3]
		seen <- request.Subject
		_ = request.Respond([]byte(`{"server":{"id":"` + serverID + `"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	requestor := NewRequestor([]string{"A", "C"}, nc, time.Second, slog.Default(), 1)
	responses, err := requestor.Request(context.Background(), VarzSubject, &struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2", len(responses))
	}
	subjects := []string{<-seen, <-seen}
	slices.Sort(subjects)
	want := []string{"$SYS.REQ.SERVER.A.VARZ", "$SYS.REQ.SERVER.C.VARZ"}
	if !slices.Equal(subjects, want) {
		t.Fatalf("subjects = %v, want %v", subjects, want)
	}
}

func TestTargetedRequestOmitsAllPerServerFailures(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe("$SYS.REQ.SERVER.*.VARZ", func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	requestor := NewRequestor([]string{"A", "B"}, nc, 20*time.Millisecond, logger, 1)

	responses, err := requestor.Request(context.Background(), VarzSubject, nil)
	if err != nil {
		t.Fatalf("Request returned aggregate error: %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("responses = %d, want all failed targets omitted", len(responses))
	}
}

func TestTargetedNonServerRequestFiltersReplies(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe(AccountStatzSubject, func(request *nats.Msg) {
		for _, id := range []string{"A", "B", "C"} {
			_ = request.Respond([]byte(`{"server":{"id":"` + id + `"}}`))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	requestor := NewRequestor([]string{"A", "C"}, nc, 40*time.Millisecond, slog.Default(), 1)
	responses, err := requestor.Request(context.Background(), AccountStatzSubject, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2", len(responses))
	}
	for _, response := range responses {
		if bytes.Contains(response, []byte(`"B"`)) {
			t.Fatalf("excluded response retained: %s", response)
		}
	}
}

func TestRequestReturnsPartialResponsesAtTimeout(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe("partial", func(request *nats.Msg) {
		_ = request.Respond([]byte(`{"server":{"id":"A"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	requestor := NewRequestor([]string{"A", "B"}, nc, 30*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	responses, err := requestor.Request(context.Background(), "partial", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 {
		t.Fatalf("responses = %d, want 1", len(responses))
	}
}

func TestRequestAdaptiveUsesQuietWindowAfterReply(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe("adaptive", func(request *nats.Msg) {
		_ = request.Respond([]byte(`{"server":{"id":"early"}}`))
		time.Sleep(100 * time.Millisecond)
		_ = request.Respond([]byte(`{"server":{"id":"late"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	requestor := NewRequestor(nil, nc, time.Second, slog.Default(), 0)
	started := time.Now()
	responses, err := requestor.RequestAdaptive(context.Background(), "adaptive", nil)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want early and late replies", len(responses))
	}
	if elapsed < 350*time.Millisecond || elapsed > 700*time.Millisecond {
		t.Fatalf("RequestAdaptive returned after %v, want 300ms after the last reply", elapsed)
	}
}

func TestRequestAdaptiveStopsBeforeReplyAfterQuietWindow(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	lateSent := make(chan struct{})
	sub, err := nc.Subscribe("adaptive.late", func(request *nats.Msg) {
		_ = request.Respond([]byte(`{"server":{"id":"early"}}`))
		time.AfterFunc(350*time.Millisecond, func() {
			_ = request.Respond([]byte(`{"server":{"id":"too-late"}}`))
			close(lateSent)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	requestor := NewRequestor(nil, nc, time.Second, slog.Default(), 0)
	started := time.Now()
	responses, err := requestor.RequestAdaptive(context.Background(), "adaptive.late", nil)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || !bytes.Contains(responses[0], []byte(`"early"`)) {
		t.Fatalf("responses = %q, want only the reply inside the quiet window", responses)
	}
	if elapsed < 250*time.Millisecond || elapsed > 750*time.Millisecond {
		t.Fatalf("RequestAdaptive returned after %v, want about 300ms after the first reply", elapsed)
	}

	select {
	case <-lateSent:
	case <-time.After(time.Second):
		t.Fatal("late fixture reply was not sent")
	}
}

func TestRequestAdaptiveOuterDeadlineWinsOverQuietWindowReset(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	repliesSent := make(chan struct{}, 3)
	sub, err := nc.Subscribe("adaptive.deadline", func(request *nats.Msg) {
		for index, delay := range []time.Duration{50 * time.Millisecond, 120 * time.Millisecond, 250 * time.Millisecond} {
			index := index
			time.AfterFunc(delay, func() {
				_ = request.Respond([]byte(fmt.Sprintf(`{"server":{"id":"S%d"}}`, index+1)))
				repliesSent <- struct{}{}
			})
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	requestor := NewRequestor(nil, nc, 200*time.Millisecond, slog.Default(), 0)
	started := time.Now()
	responses, err := requestor.RequestAdaptive(context.Background(), "adaptive.deadline", nil)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %q, want the two replies before the fixed outer deadline", responses)
	}
	if elapsed < 170*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("RequestAdaptive returned after %v, want the fixed 200ms outer deadline", elapsed)
	}

	for range 3 {
		select {
		case <-repliesSent:
		case <-time.After(time.Second):
			t.Fatal("fixture did not send all scheduled replies")
		}
	}
}

func TestRequestAdaptiveWithoutRepliesUsesInitialTimeout(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe("adaptive.none", func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	requestor := NewRequestor(nil, nc, 80*time.Millisecond, slog.Default(), 0)

	started := time.Now()
	responses, err := requestor.RequestAdaptive(context.Background(), "adaptive.none", nil)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 0 {
		t.Fatalf("responses = %d, want none", len(responses))
	}
	if elapsed < 65*time.Millisecond {
		t.Fatalf("RequestAdaptive returned after %v, want initial timeout", elapsed)
	}
}

func TestExactRequestTimeoutIsNormalCompletion(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe("ordinary", func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	requestor := NewRequestor([]string{"A"}, nc, 20*time.Millisecond, logger, 0)

	responses, err := requestor.Request(context.Background(), "ordinary", nil)
	if err != nil {
		t.Fatalf("Request error = %v, want normal completion", err)
	}
	if len(responses) != 0 {
		t.Fatalf("responses = %d, want none", len(responses))
	}
}

func TestNoRespondersErrorDependsOnSystemSubject(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	requestor := NewRequestor([]string{"A"}, nc, time.Second, logger, 0)

	_, err := requestor.Request(context.Background(), "ordinary.no.responder", nil)
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("ordinary subject error = %v, want nats.ErrNoResponders", err)
	}
	_, err = requestor.Request(context.Background(), "$SYS.NO.RESPONDER", nil)
	if err == nil || err.Error() != systemRequestError {
		t.Fatalf("system subject error = %v, want %q", err, systemRequestError)
	}
}

func TestRequestPayloadPreservesRawBytesAndNilObject(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	payloads := make(chan []byte, 2)
	sub, err := nc.Subscribe("payload", func(request *nats.Msg) {
		payloads <- append([]byte(nil), request.Data...)
		_ = request.Respond([]byte(`{}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	requestor := NewRequestor([]string{"A"}, nc, time.Second, slog.Default(), 0)
	raw := []byte(`{"already":"encoded"}`)
	if _, err := requestor.Request(context.Background(), "payload", raw); err != nil {
		t.Fatal(err)
	}
	if got := <-payloads; !bytes.Equal(got, raw) {
		t.Fatalf("raw payload = %q, want %q", got, raw)
	}
	if _, err := requestor.Request(context.Background(), "payload", nil); err != nil {
		t.Fatal(err)
	}
	if got := <-payloads; string(got) != "{}" {
		t.Fatalf("nil payload = %q, want {}", got)
	}
}

func TestRequestMarshalErrorOccursBeforeNetworkSetup(t *testing.T) {
	_, err := doReq(
		context.Background(),
		make(chan int),
		"not.published",
		1,
		nil,
		time.Second,
		slog.Default(),
	)
	var unsupported *json.UnsupportedTypeError
	if !errors.As(err, &unsupported) {
		t.Fatalf("marshal error = %v, want json.UnsupportedTypeError", err)
	}
}

func TestRequestClosedConnectionReturnsSubscribeError(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	nc.Close()
	err := doReqAsync(
		context.Background(),
		nil,
		"closed",
		1,
		nc,
		time.Second,
		slog.Default(),
		func([]byte) {},
	)
	if !errors.Is(err, nats.ErrConnectionClosed) {
		t.Fatalf("closed connection error = %v, want nats.ErrConnectionClosed", err)
	}
}

func TestInvalidSnappyErrorPrecedesNoRespondersStatus(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe("$SYS.INVALID.SNAPPY", func(request *nats.Msg) {
		response := nats.NewMsg(request.Reply)
		response.Header.Set("Content-Encoding", "snappy")
		response.Header.Set("Status", "503")
		response.Data = []byte("invalid-s2-frame")
		_ = nc.PublishMsg(response)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	handled := false
	err = doReqAsync(
		context.Background(),
		nil,
		"$SYS.INVALID.SNAPPY",
		1,
		nc,
		time.Second,
		slog.Default(),
		func([]byte) { handled = true },
	)
	if err == nil || errors.Is(err, nats.ErrNoResponders) || err.Error() == systemRequestError {
		t.Fatalf("response error = %v, want S2 decode failure before 503 translation", err)
	}
	if handled {
		t.Fatal("handler called for invalid compressed response")
	}
}

func runTestNATSServer(t *testing.T, jetStream bool) (*server.Server, *nats.Conn) {
	t.Helper()
	options := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: jetStream,
	}
	if jetStream {
		options.StoreDir = t.TempDir()
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
	return srv, nc
}

func snappyFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := s2.NewWriter(&output, s2.WriterSnappyCompat(), s2.WriterConcurrency(1))
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
