package sink

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var _ func(string, int64) (*nats.Msg, error) = PreparePublish

func TestCompressorLayoutMatchesBinary(t *testing.T) {
	var compressor Compressor
	if got := unsafe.Sizeof(compressor); got != 0x08 {
		t.Fatalf("Compressor size = %#x, want %#x", got, uintptr(0x08))
	}
	if got := unsafe.Offsetof(compressor.writer); got != 0 {
		t.Fatalf("writer offset = %#x, want 0", got)
	}
}

func TestDecompressorLayoutMatchesBinary(t *testing.T) {
	var decompressor Decompressor
	if got := unsafe.Sizeof(decompressor); got != 0x10 {
		t.Fatalf("Decompressor size = %#x, want %#x", got, uintptr(0x10))
	}
	if got := unsafe.Offsetof(decompressor.in); got != 0 {
		t.Fatalf("in offset = %#x, want 0", got)
	}
	if got := unsafe.Offsetof(decompressor.s2r); got != 8 {
		t.Fatalf("s2r offset = %#x, want 8", got)
	}
	typeOf := reflect.TypeOf(decompressor)
	for i, want := range []string{"in", "s2r"} {
		if got := typeOf.Field(i).Name; got != want {
			t.Errorf("field %d name = %q, want %q", i, got, want)
		}
	}
}

func TestPrepareBatchAndParseRoundTrip(t *testing.T) {
	payload := []byte(`{"server":{"id":"S1","name":"one"},"data":{"connections":1}}`)
	compressor := NewCompressor()
	defer compressor.Close()
	messages, err := PrepareBatch(compressor, "scrape.123.varz", 123, [][]byte{payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(messages))
	}
	msg := messages[0]
	if msg.Header.Get(ContentEncodingHeader) != SnappyEncoding {
		t.Fatalf("Content-Encoding = %q", msg.Header.Get(ContentEncodingHeader))
	}
	if msg.Header.Get(EpochHeader) != "123" || msg.Header.Get(ServerHeader) != "S1" {
		t.Fatalf("headers = %#v", msg.Header)
	}
	if bytes.Equal(msg.Data, payload) {
		t.Fatal("payload was not compressed")
	}

	value, endpoint, epoch, err := Parse(NewDecompressor(), &bytes.Buffer{}, msg.Subject, msg.Data, msg.Header)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "varz" || epoch != 123 {
		t.Fatalf("endpoint/epoch = %q/%d", endpoint, epoch)
	}
	response, ok := value.(*server.ServerAPIVarzResponse)
	if !ok || response.Server == nil || response.Server.ID != "S1" {
		t.Fatalf("parsed response = %#v", value)
	}
}

func TestPreparePublishAndParseMarker(t *testing.T) {
	msg, err := PreparePublish("scrape.456.end", 456)
	if err != nil {
		t.Fatal(err)
	}
	value, endpoint, epoch, err := Parse(NewDecompressor(), &bytes.Buffer{}, msg.Subject, nil, msg.Header)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil || endpoint != "end" || epoch != 456 {
		t.Fatalf("marker = %#v %q %d", value, endpoint, epoch)
	}
}

func TestPrepareBatchEmptyResultIsNonNil(t *testing.T) {
	compressor := NewCompressor()
	defer compressor.Close()
	messages, err := PrepareBatch(compressor, "scrape.1.varz", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if messages == nil || len(messages) != 0 {
		t.Fatalf("messages = %#v, want non-nil empty slice", messages)
	}
}

func TestParseRejectsMissingEpochAndUnknownEndpoint(t *testing.T) {
	if _, _, _, err := Parse(NewDecompressor(), &bytes.Buffer{}, "scrape.1.varz", nil, nats.Header{}); err == nil || err.Error() != "missing epoch header" {
		t.Fatalf("missing epoch error = %v", err)
	}
	headers := nats.Header{}
	headers.Set(EpochHeader, "1")
	data := compressForTest(t, []byte(`{}`))
	if _, _, _, err := Parse(NewDecompressor(), &bytes.Buffer{}, "scrape.1.unknown", data, headers); err == nil || err.Error() != "unknown endpoint: unknown" {
		t.Fatalf("unknown endpoint error = %v", err)
	}
}

func TestParseAlwaysDecompressesPayload(t *testing.T) {
	headers := nats.Header{}
	headers.Set(EpochHeader, "1")
	data := compressForTest(t, []byte(`{"server":{"id":"S1"}}`))

	value, endpoint, epoch, err := Parse(NewDecompressor(), &bytes.Buffer{}, "scrape.1.varz", data, headers)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value.(*server.ServerAPIVarzResponse); !ok || endpoint != "varz" || epoch != 1 {
		t.Fatalf("parsed value/endpoint/epoch = %T/%q/%d", value, endpoint, epoch)
	}

	_, _, _, err = Parse(NewDecompressor(), &bytes.Buffer{}, "scrape.1.varz", []byte(`{}`), headers)
	if err == nil || strings.Contains(err.Error(), "decompress payload") {
		t.Fatalf("raw payload error = %v", err)
	}
}

func TestParseErrorPrecedenceMatchesBinary(t *testing.T) {
	invalidEpoch := nats.Header{}
	invalidEpoch.Set(EpochHeader, "not-an-integer")
	if _, _, _, err := Parse(
		NewDecompressor(),
		&bytes.Buffer{},
		"scrape.not-an-integer.end",
		[]byte("not-s2"),
		invalidEpoch,
	); err == nil || !strings.HasPrefix(err.Error(), "parse epoch header: ") {
		t.Fatalf("invalid epoch error = %v", err)
	}

	validEpoch := nats.Header{}
	validEpoch.Set(EpochHeader, "1")
	value, endpoint, epoch, err := Parse(
		NewDecompressor(),
		&bytes.Buffer{},
		"scrape.1.end",
		[]byte("not-s2"),
		validEpoch,
	)
	if err != nil || value != nil || endpoint != "end" || epoch != 1 {
		t.Fatalf("marker parse = %#v/%q/%d/%v", value, endpoint, epoch, err)
	}

	_, _, _, err = Parse(
		NewDecompressor(),
		&bytes.Buffer{},
		"scrape.1.unknown",
		[]byte("not-s2"),
		validEpoch,
	)
	if err == nil || strings.Contains(err.Error(), "unknown endpoint") {
		t.Fatalf("unknown endpoint with invalid frame error = %v, want S2 error", err)
	}
}

func TestParseAccountStatzUsesGenericEnvelope(t *testing.T) {
	headers := nats.Header{}
	headers.Set(EpochHeader, "1")
	data := compressForTest(t, []byte(`{"server":{"id":"S1"},"data":{"num_accounts":2}}`))

	value, _, _, err := Parse(NewDecompressor(), &bytes.Buffer{}, "scrape.1.accstatz", data, headers)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := value.(*server.ServerAPIResponse)
	if !ok {
		t.Fatalf("parsed response type = %T, want *server.ServerAPIResponse", value)
	}
	if _, ok := response.Data.(*server.AccountStatz); !ok {
		t.Fatalf("parsed response data type = %T, want *server.AccountStatz", response.Data)
	}
}

func TestParseWrapsJSONErrorWithEndpoint(t *testing.T) {
	headers := nats.Header{}
	headers.Set(EpochHeader, "1")
	data := compressForTest(t, []byte(`{`))

	_, _, _, err := Parse(NewDecompressor(), &bytes.Buffer{}, "scrape.1.varz", data, headers)
	if err == nil || !strings.HasPrefix(err.Error(), "unmarshal varz: ") {
		t.Fatalf("unmarshal error = %v", err)
	}
}

func TestCompressorCanBeReused(t *testing.T) {
	compressor := NewCompressor()
	decompressor := NewDecompressor()
	output := &bytes.Buffer{}
	for _, input := range [][]byte{[]byte("first"), []byte("second payload")} {
		var compressed bytes.Buffer
		if err := compressor.Compress(&compressed, input); err != nil {
			t.Fatal(err)
		}
		if err := decompressor.Decompress(output, compressed.Bytes()); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(output.Bytes(), input) {
			t.Fatalf("decoded = %q, want %q", output.Bytes(), input)
		}
	}
}

func compressForTest(t *testing.T, input []byte) []byte {
	t.Helper()
	compressor := NewCompressor()
	defer compressor.Close()
	var compressed bytes.Buffer
	if err := compressor.Compress(&compressed, input); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}
