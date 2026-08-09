package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"github.com/klauspost/compress/s2"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"
)

const (
	ContentEncodingHeader = "Content-Encoding"
	EpochHeader           = "X-Epoch"
	ServerHeader          = "X-Server"
	SnappyEncoding        = "snappy"
)

type PrepareFunc func(
	context.Context,
	*slog.Logger,
	*nats.Conn,
	string,
	int64,
	[][]byte,
	int,
) (int, error)

type PublishFunc func(
	context.Context,
	*slog.Logger,
	*nats.Conn,
	string,
	int64,
) error

type Compressor struct {
	writer *s2.Writer
}

func NewCompressor() *Compressor {
	return &Compressor{writer: s2.NewWriter(nil)}
}

func (c *Compressor) Compress(output io.Writer, data []byte) error {
	c.writer.Reset(output)
	if _, err := c.writer.Write(data); err != nil {
		return err
	}
	return c.writer.Close()
}

func (c *Compressor) Close() error {
	return c.writer.Close()
}

type Decompressor struct {
	in  *bytes.Buffer
	s2r *s2.Reader
}

func NewDecompressor() *Decompressor {
	buffer := &bytes.Buffer{}
	return &Decompressor{
		in:  buffer,
		s2r: s2.NewReader(buffer),
	}
}

func (d *Decompressor) Decompress(output *bytes.Buffer, data []byte) error {
	d.in.Reset()
	if _, err := d.in.Write(data); err != nil {
		return err
	}
	d.s2r.Reset(d.in)
	output.Reset()
	_, err := output.ReadFrom(d.s2r)
	return err
}

func PrepareBatch(compressor *Compressor, subject string, epoch int64, payloads [][]byte) ([]*nats.Msg, error) {
	messages := make([]*nats.Msg, 0, len(payloads))
	for _, payload := range payloads {
		var compressed bytes.Buffer
		if err := compressor.Compress(&compressed, payload); err != nil {
			return nil, err
		}
		msg := nats.NewMsg(subject)
		msg.Data = compressed.Bytes()
		msg.Header.Set(ContentEncodingHeader, SnappyEncoding)
		msg.Header.Set(EpochHeader, strconv.FormatInt(epoch, 10))
		msg.Header.Set(ServerHeader, gjson.GetBytes(payload, "server.id").String())
		messages = append(messages, msg)
	}
	return messages, nil
}

func PreparePublish(subject string, epoch int64) (*nats.Msg, error) {
	msg := nats.NewMsg(subject)
	msg.Header.Set(EpochHeader, strconv.FormatInt(epoch, 10))
	return msg, nil
}

// Parse converts a sink message into the typed NATS monitoring response used
// by downstream processors. Start and end markers return a nil value.
func Parse(
	decompressor *Decompressor,
	buffer *bytes.Buffer,
	subject string,
	data []byte,
	headers nats.Header,
) (any, string, int64, error) {
	epochValue := headers.Get(EpochHeader)
	if epochValue == "" {
		return nil, "", 0, errors.New("missing epoch header")
	}
	epoch, err := strconv.ParseInt(epochValue, 10, 64)
	if err != nil {
		return nil, "", 0, fmt.Errorf("parse epoch header: %w", err)
	}

	tokens := strings.Split(subject, ".")
	if len(tokens) == 0 {
		return nil, "", 0, errors.New("unknown endpoint: ")
	}
	endpoint := tokens[len(tokens)-1]
	if endpoint == "start" || endpoint == "end" {
		return nil, endpoint, epoch, nil
	}

	if err := decompressor.Decompress(buffer, data); err != nil {
		return nil, "", 0, err
	}
	data = buffer.Bytes()

	var value any
	switch endpoint {
	case "servers":
		value = &server.ServerStatsMsg{}
	case "varz":
		value = &server.ServerAPIVarzResponse{}
	case "connz":
		value = &server.ServerAPIConnzResponse{}
	case "jsz":
		value = &server.ServerAPIJszResponse{}
	case "routez":
		value = &server.ServerAPIRoutezResponse{}
	case "subsz":
		value = &server.ServerAPISubszResponse{}
	case "gatewayz":
		value = &server.ServerAPIGatewayzResponse{}
	case "leafz":
		value = &server.ServerAPILeafzResponse{}
	case "accountz":
		value = &server.ServerAPIAccountzResponse{}
	case "accstatz":
		value = &server.ServerAPIResponse{Data: &server.AccountStatz{}}
	case "healthz":
		value = &server.ServerAPIHealthzResponse{}
	case "raftz":
		value = &server.ServerAPIRaftzResponse{}
	case "ipqueuesz":
		value = &server.ServerAPIpqueueszResponse{}
	default:
		return nil, "", 0, fmt.Errorf("unknown endpoint: %s", endpoint)
	}
	if err := json.Unmarshal(data, value); err != nil {
		return nil, "", 0, fmt.Errorf("unmarshal %s: %w", endpoint, err)
	}
	return value, endpoint, epoch, nil
}
