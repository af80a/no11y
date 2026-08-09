package noop

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go"
)

func Prepare[T any](
	_ context.Context,
	_ *slog.Logger,
	_ *nats.Conn,
	_ string,
	_ int64,
	payloads []T,
	_ int,
) (int, error) {
	return len(payloads), nil
}

func Publish(context.Context, *slog.Logger, *nats.Conn, string, int64) error {
	return nil
}
