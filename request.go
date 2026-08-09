package scraper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"
)

const systemRequestError = "server request failed, ensure the account used has system privileges and appropriate permissions"

type requestor struct {
	ids      []string
	nc       *nats.Conn
	timeout  time.Duration
	logger   *slog.Logger
	targeted bool
	included map[string]struct{}
}

func (r *requestor) Request(ctx context.Context, subject string, options any) ([][]byte, error) {
	if r.targeted {
		return r.requestTargeted(ctx, subject, options)
	}
	return doReq(ctx, options, subject, len(r.ids), r.nc, r.timeout, r.logger)
}

func (r *requestor) RequestOne(ctx context.Context, subject string, options any) ([]byte, error) {
	responses, err := doReq(ctx, options, subject, 1, r.nc, r.timeout, r.logger)
	if err != nil {
		return nil, err
	}
	if len(responses) == 0 {
		return nil, errors.New("no response received from server")
	}
	return responses[0], nil
}

func (r *requestor) ServerCount() int { return len(r.ids) }

func (r *requestor) RequestAdaptive(ctx context.Context, subject string, options any) ([][]byte, error) {
	if r.targeted {
		return r.requestTargeted(ctx, subject, options)
	}
	return doReq(ctx, options, subject, 0, r.nc, r.timeout, r.logger)
}

func (r *requestor) Timeout() time.Duration { return r.timeout }

func (r *requestor) requestTargeted(ctx context.Context, subject string, options any) ([][]byte, error) {
	const pingPrefix = "$SYS.REQ.SERVER.PING."
	if strings.HasPrefix(subject, pingPrefix) {
		endpoint := strings.TrimPrefix(subject, pingPrefix)
		return r.requestPerServer(ctx, endpoint, options)
	}
	return r.requestFiltered(ctx, subject, options)
}

func (r *requestor) requestPerServer(ctx context.Context, endpoint string, options any) ([][]byte, error) {
	var (
		mu        sync.Mutex
		wg        sync.WaitGroup
		responses [][]byte
	)

	for _, id := range r.ids {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			subject := fmt.Sprintf("$SYS.REQ.SERVER.%s.%s", id, endpoint)
			response, err := doReq(ctx, options, subject, 1, r.nc, r.timeout, r.logger)
			if err != nil {
				r.logger.Warn("scrape/targeted", "server", id, "endpoint", endpoint, "error", err)
				return
			}
			mu.Lock()
			responses = append(responses, response...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	return responses, nil
}

func (r *requestor) requestFiltered(ctx context.Context, subject string, options any) ([][]byte, error) {
	responses, err := doReq(ctx, options, subject, 0, r.nc, r.timeout, r.logger)
	if err != nil {
		return nil, err
	}
	return filterByServerID(responses, r.included), nil
}

func filterByServerID(responses [][]byte, included map[string]struct{}) [][]byte {
	filtered := make([][]byte, 0, len(responses))
	for _, response := range responses {
		id := gjson.GetBytes(response, "server.id").String()
		if _, ok := included[id]; ok {
			filtered = append(filtered, response)
		}
	}
	return filtered
}

func doReq(
	ctx context.Context,
	options any,
	subject string,
	expected int,
	nc *nats.Conn,
	timeout time.Duration,
	logger *slog.Logger,
) ([][]byte, error) {
	var (
		mu        sync.Mutex
		responses [][]byte
	)
	err := doReqAsync(ctx, options, subject, expected, nc, timeout, logger, func(response []byte) {
		mu.Lock()
		responses = append(responses, response)
		mu.Unlock()
	})
	return responses, err
}

func doReqAsync(
	ctx context.Context,
	options any,
	subject string,
	expected int,
	nc *nats.Conn,
	timeout time.Duration,
	logger *slog.Logger,
	handler func([]byte),
) error {
	payload := []byte("{}")
	if options != nil {
		if raw, ok := options.([]byte); ok {
			payload = raw
		} else {
			var err error
			payload, err = json.Marshal(options)
			if err != nil {
				return err
			}
		}
	}

	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var timer *time.Timer
	if expected == 0 {
		timer = time.NewTimer(timeout)
		go func() {
			select {
			case <-timer.C:
				cancel()
			case <-requestCtx.Done():
			}
		}()
	}

	var (
		mu       sync.Mutex
		received int
	)
	errCh := make(chan error)
	inbox := nc.NewRespInbox()
	sub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()

		data := msg.Data
		if msg.Header.Get("Content-Encoding") == "snappy" {
			decoded, err := io.ReadAll(s2.NewReader(bytes.NewBuffer(data)))
			if err != nil {
				errCh <- err
				return
			}
			data = decoded
		}
		if timer != nil {
			timer.Reset(300 * time.Millisecond)
		}
		if msg.Header.Get("Status") == "503" {
			errCh <- nats.ErrNoResponders
			return
		}

		handler(data)
		received++
		if expected > 0 && received == expected {
			cancel()
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	if expected > 0 {
		if err := sub.AutoUnsubscribe(expected); err != nil {
			return err
		}
	}
	msg := &nats.Msg{
		Subject: subject,
		Reply:   inbox,
		Data:    payload,
	}
	if subject != ServerPingSubject && !strings.HasPrefix(subject, "$SYS.REQ.ACCOUNT") {
		msg.Header = nats.Header{"Accept-Encoding": []string{"snappy"}}
	}
	if err := nc.PublishMsg(msg); err != nil {
		return err
	}

	select {
	case err := <-errCh:
		if errors.Is(err, nats.ErrNoResponders) && strings.HasPrefix(subject, "$SYS") {
			return fmt.Errorf(systemRequestError)
		}
		return err
	case <-requestCtx.Done():
		mu.Lock()
		if expected > 0 && expected > received {
			logger.Warn("scrape/timeout", "subject", subject, "expected", expected, "received", received)
		}
		mu.Unlock()
	}
	return nil
}
