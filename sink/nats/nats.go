package nats

import (
	"context"
	"log/slog"

	"github.com/johndoe/nats-scraper/sink"
	natsgo "github.com/nats-io/nats.go"
)

func Publish(
	ctx context.Context,
	_ *slog.Logger,
	nc *natsgo.Conn,
	subject string,
	epoch int64,
) error {
	_ = ctx
	message, err := sink.PreparePublish(subject, epoch)
	if err != nil {
		return err
	}
	return nc.PublishMsg(message)
}

func Prepare(
	ctx context.Context,
	_ *slog.Logger,
	nc *natsgo.Conn,
	subject string,
	epoch int64,
	payloads [][]byte,
	batchSize int,
) (int, error) {
	_ = ctx
	_ = batchSize
	compressor := sink.NewCompressor()
	messages, err := sink.PrepareBatch(compressor, subject, epoch, payloads)
	if err != nil {
		return 0, err
	}
	if err := publishBatch(nc, messages); err != nil {
		return 0, err
	}
	return len(payloads), nil
}

func publishBatch(nc *natsgo.Conn, messages []*natsgo.Msg) error {
	for _, message := range messages {
		if err := nc.PublishMsg(message); err != nil {
			return err
		}
	}
	return nil
}
