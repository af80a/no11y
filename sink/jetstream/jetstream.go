package jetstream

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/johndoe/nats-scraper/sink"
	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

type Parser struct {
	dec *sink.Decompressor
	buf *bytes.Buffer
}

func NewParser() *Parser {
	return &Parser{
		dec: sink.NewDecompressor(),
		buf: &bytes.Buffer{},
	}
}

func (p *Parser) Parse(msg jsapi.Msg) (any, string, int64, error) {
	return sink.Parse(p.dec, p.buf, msg.Subject(), msg.Data(), msg.Headers())
}

func Publish(
	ctx context.Context,
	_ *slog.Logger,
	nc *nats.Conn,
	subject string,
	epoch int64,
) error {
	js, err := jsapi.New(nc)
	if err != nil {
		return err
	}
	message, err := sink.PreparePublish(subject, epoch)
	if err != nil {
		return err
	}
	_, err = js.PublishMsg(ctx, message)
	return err
}

func Prepare(
	ctx context.Context,
	logger *slog.Logger,
	nc *nats.Conn,
	subject string,
	epoch int64,
	payloads [][]byte,
	batchSize int,
) (int, error) {
	js, err := jsapi.New(nc)
	if err != nil {
		return 0, err
	}
	compressor := sink.NewCompressor()
	defer compressor.Close()
	messages, err := sink.PrepareBatch(compressor, subject, epoch, payloads)
	if err != nil {
		return 0, err
	}
	count, err := publishBatch(ctx, logger, js, messages, batchSize)
	defer js.CleanupPublisher()
	return count, err
}

func waitPublish(ctx context.Context, logger *slog.Logger, js jsapi.JetStream, futures []jsapi.PubAckFuture) {
	<-js.PublishAsyncComplete()
	for _, future := range futures {
		select {
		case <-ctx.Done():
			return
		case err := <-future.Err():
			if err != nil {
				logger.Error("stream/publish", "subject", future.Msg().Subject, "error", err)
			}
		case <-future.Ok():
		}
	}
}

func publishBatch(
	ctx context.Context,
	logger *slog.Logger,
	js jsapi.JetStream,
	messages []*nats.Msg,
	batchSize int,
) (int, error) {
	var futures []jsapi.PubAckFuture
	for _, message := range messages {
		future, err := js.PublishMsgAsync(message)
		if err != nil {
			return 0, err
		}
		futures = append(futures, future)
		if len(futures) == batchSize {
			waitPublish(ctx, logger, js, futures)
			futures = nil
		}
	}
	if len(futures) != 0 {
		waitPublish(ctx, logger, js, futures)
	}
	return len(messages), nil
}

func LastEpoch(ctx context.Context, nc *nats.Conn, streamName, subjectPrefix string) (int64, error) {
	js, err := jsapi.New(nc)
	if err != nil {
		return 0, err
	}
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return 0, err
	}
	if stream.CachedInfo().State.Msgs == 0 {
		return 0, nil
	}
	consumer, err := stream.OrderedConsumer(ctx, jsapi.OrderedConsumerConfig{
		FilterSubjects: []string{fmt.Sprintf("%s.*.end", subjectPrefix)},
		DeliverPolicy:  jsapi.DeliverLastPolicy,
		HeadersOnly:    true,
	})
	if err != nil {
		return 0, err
	}
	defer js.DeleteConsumer(ctx, streamName, consumer.CachedInfo().Name)

	batch, err := consumer.Fetch(1, jsapi.FetchMaxWait(2*time.Second))
	if err != nil {
		return 0, err
	}
	for msg := range batch.Messages() {
		value := msg.Headers().Get(sink.EpochHeader)
		if value == "" {
			return 0, nil
		}
		epoch, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, err
		}
		return epoch, nil
	}
	if err := batch.Error(); err != nil {
		return 0, err
	}
	return 0, nil
}

func Prune(ctx context.Context, nc *nats.Conn, streamName, subjectPrefix string, maxEpochs int) (int, error) {
	if maxEpochs <= 0 {
		return 0, nil
	}
	js, err := jsapi.New(nc)
	if err != nil {
		return 0, err
	}
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return 0, err
	}
	if stream.CachedInfo().State.FirstSeq == 0 {
		return 0, nil
	}
	consumer, err := stream.OrderedConsumer(ctx, jsapi.OrderedConsumerConfig{
		FilterSubjects: []string{fmt.Sprintf("%s.*.end", subjectPrefix)},
		DeliverPolicy:  jsapi.DeliverAllPolicy,
		HeadersOnly:    true,
	})
	if err != nil {
		return 0, err
	}
	defer js.DeleteConsumer(ctx, streamName, consumer.CachedInfo().Name)

	messages, err := consumer.Messages()
	if err != nil {
		return 0, err
	}
	defer messages.Stop()

	endSequences := make([]uint64, 0, maxEpochs)
	previousEpoch := ""
	pruned := 0
	for {
		msg, err := messages.Next()
		if err != nil {
			return 0, err
		}
		metadata, _ := msg.Metadata()
		epoch := msg.Headers().Get(sink.EpochHeader)
		if epoch != previousEpoch {
			if len(endSequences) == maxEpochs {
				endSequences = endSequences[1:]
				pruned++
			}
			endSequences = append(endSequences, metadata.Sequence.Stream)
		}
		previousEpoch = epoch
		if metadata.NumPending == 0 {
			break
		}
	}
	if len(endSequences) == 0 || pruned <= 0 {
		return pruned, nil
	}
	if err := stream.Purge(ctx, jsapi.WithPurgeSequence(endSequences[0])); err != nil {
		return 0, err
	}
	return pruned, nil
}
