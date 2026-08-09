package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"
)

func CurrentActiveServerInfos(
	ctx context.Context,
	logger *slog.Logger,
	nc *nats.Conn,
	timeout time.Duration,
) ([]ServerInfo, error) {
	seen := make(map[string]struct{})
	var result []ServerInfo
	err := doReqAsync(
		ctx,
		nil,
		ServerPingSubject,
		-1,
		nc,
		clampEntityTimeout(timeout),
		logger,
		func(response []byte) {
			var message server.ServerStatsMsg
			if err := json.Unmarshal(response, &message); err != nil {
				return
			}
			if _, ok := seen[message.Server.ID]; ok {
				return
			}
			seen[message.Server.ID] = struct{}{}
			result = append(result, ServerInfo{
				ID:      message.Server.ID,
				Name:    message.Server.Name,
				Cluster: message.Server.Cluster,
				Tags:    message.Server.Tags,
			})
		},
	)
	return result, err
}

func Servers(ctx context.Context, requestor Requestor) ([][]byte, error) {
	return requestor.Request(ctx, ServerPingSubject, nil)
}

func Varz(ctx context.Context, logger *slog.Logger, requestor Requestor, _ *VarzOptions) ([][]byte, error) {
	responses, err := requestor.Request(ctx, VarzSubject, &server.VarzEventOptions{})
	if err != nil {
		return nil, err
	}
	var result [][]byte
	for _, response := range responses {
		if gjson.GetBytes(response, "error").Exists() {
			logger.Error(
				"varz/page",
				"error", gjson.GetBytes(response, "error.description").String(),
				"server", gjson.GetBytes(response, "server.name").String(),
			)
			continue
		}
		result = append(result, response)
		logger.Debug("varz/page", "size", len(response))
	}
	return result, nil
}

func Connz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *ConnzOptions) ([][]byte, error) {
	requestOptions := options.toServerOptions(0)
	var result [][]byte
	for {
		responses, err := requestor.Request(ctx, ConnzSubject, requestOptions)
		if err != nil {
			return nil, err
		}
		more := false
		for _, response := range responses {
			if gjson.GetBytes(response, "error").Exists() {
				logger.Error(
					"connz/page",
					"error", gjson.GetBytes(response, "error.description").String(),
					"server", gjson.GetBytes(response, "server.name").String(),
				)
				continue
			}
			result = append(result, response)
			total := int(gjson.GetBytes(response, "data.total").Int())
			logger.Debug("connz/page", "size", len(response))
			more = total > requestOptions.Offset+requestOptions.Limit
		}
		if !more {
			return result, nil
		}
		requestOptions.Offset += requestOptions.Limit + 1
	}
}

func Subsz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *SubszOptions) ([][]byte, error) {
	requestOptions := options.toServerOptions(0)
	var result [][]byte
	for {
		responses, err := requestor.Request(ctx, SubszSubject, requestOptions)
		if err != nil {
			return nil, err
		}
		more := false
		for _, response := range responses {
			if gjson.GetBytes(response, "error").Exists() {
				logger.Error(
					"subsz/page",
					"error", gjson.GetBytes(response, "error.description").String(),
					"server", gjson.GetBytes(response, "server.name").String(),
				)
				continue
			}
			result = append(result, response)
			total := int(gjson.GetBytes(response, "data.total").Int())
			logger.Debug("subsz/page", "size", len(response))
			more = total > requestOptions.Offset+requestOptions.Limit
		}
		if !more {
			return result, nil
		}
		requestOptions.Offset += requestOptions.Limit + 1
	}
}

func Routez(ctx context.Context, logger *slog.Logger, requestor Requestor, options *RoutezOptions) ([][]byte, error) {
	responses, err := requestor.Request(ctx, RoutezSubject, options.toServerOptions())
	if err != nil {
		return nil, err
	}
	var result [][]byte
	for _, response := range responses {
		if gjson.GetBytes(response, "error").Exists() {
			logger.Error(
				"routez/page",
				"error", gjson.GetBytes(response, "error.description").String(),
				"server", gjson.GetBytes(response, "server.name").String(),
			)
			continue
		}
		result = append(result, response)
		logger.Debug("routez/page", "size", len(response))
	}
	return result, nil
}

func Jsz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *JszOptions) ([][]byte, error) {
	requestOptions := options.toServerOptions(0)
	var result [][]byte
	for {
		responses, err := requestor.Request(ctx, JszSubject, requestOptions)
		if err != nil {
			return nil, err
		}
		more := false
		for _, response := range responses {
			if gjson.GetBytes(response, "error").Exists() {
				logger.Error(
					"jsz/page",
					"error", gjson.GetBytes(response, "error.description").String(),
					"server", gjson.GetBytes(response, "server.name").String(),
				)
				continue
			}
			result = append(result, response)
			total := int(gjson.GetBytes(response, "data.accounts").Int())
			logger.Debug("jsz/page", "size", len(response))
			more = total > requestOptions.Offset+requestOptions.Limit
		}
		if !more {
			return result, nil
		}
		requestOptions.Offset += requestOptions.Limit
	}
}

func Gatewayz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *GatewayzOptions) ([][]byte, error) {
	responses, err := requestor.Request(ctx, GatewayzSubject, options.toServerOptions())
	if err != nil {
		return nil, err
	}
	var result [][]byte
	for _, response := range responses {
		if gjson.GetBytes(response, "error").Exists() {
			logger.Error(
				"gatewayz/page",
				"error", gjson.GetBytes(response, "error.description").String(),
				"server", gjson.GetBytes(response, "server.name").String(),
			)
			continue
		}
		result = append(result, response)
		logger.Debug("gatewayz/page", "size", len(response))
	}
	return result, nil
}

func Leafz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *LeafzOptions) ([][]byte, error) {
	responses, err := requestor.Request(ctx, LeafzSubject, options.toServerOptions())
	if err != nil {
		return nil, err
	}
	var result [][]byte
	for _, response := range responses {
		if gjson.GetBytes(response, "error").Exists() {
			logger.Error(
				"leafz/page",
				"error", gjson.GetBytes(response, "error.description").String(),
				"server", gjson.GetBytes(response, "server.name").String(),
			)
			continue
		}
		result = append(result, response)
		logger.Debug("leafz/page", "size", len(response))
	}
	return result, nil
}

func Accstatz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *AccountStatzOptions) ([][]byte, error) {
	responses, err := requestor.Request(ctx, AccountStatzSubject, options.toServerOptions())
	if err != nil {
		return nil, err
	}
	var result [][]byte
	for _, response := range responses {
		if gjson.GetBytes(response, "error").Exists() {
			logger.Error(
				"accstatz/page",
				"error", gjson.GetBytes(response, "error.description").String(),
				"server", gjson.GetBytes(response, "server.name").String(),
			)
			continue
		}
		result = append(result, response)
		logger.Debug("accstatz/page", "size", len(response))
	}
	return result, nil
}

func Healthz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *HealthzOptions) ([][]byte, error) {
	responses, err := requestor.Request(ctx, HealthzSubject, options.toServerOptions())
	if err != nil {
		return nil, err
	}
	var result [][]byte
	for _, response := range responses {
		if gjson.GetBytes(response, "error").Exists() {
			logger.Error(
				"healthz/page",
				"error", gjson.GetBytes(response, "error.description").String(),
				"server", gjson.GetBytes(response, "server.name").String(),
			)
			continue
		}
		result = append(result, response)
		logger.Debug("healthz/page", "size", len(response))
	}
	return result, nil
}

func Ipqueuesz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *IpqueueszOptions) ([][]byte, error) {
	responses, err := requestor.Request(ctx, IpqueueszSubject, options.toServerOptions())
	if err != nil {
		return nil, err
	}
	var result [][]byte
	for _, response := range responses {
		if gjson.GetBytes(response, "error").Exists() {
			logger.Error(
				"ipqueuesz/page",
				"error", gjson.GetBytes(response, "error.description").String(),
				"server", gjson.GetBytes(response, "server.name").String(),
			)
			continue
		}
		result = append(result, response)
		logger.Debug("ipqueuesz/page", "size", len(response))
	}
	return result, nil
}

func Accountz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *AccountzOptions) ([][]byte, error) {
	discoveryStarted := time.Now()
	inventory, err := requestor.Request(ctx, AccountzSubject, options.toServerOptions())
	if err != nil {
		return nil, err
	}
	accounts := make(map[string][]string)
	for _, response := range inventory {
		var envelope server.ServerAPIAccountzResponse
		if err := json.Unmarshal(response, &envelope); err != nil {
			continue
		}
		if envelope.Error != nil || envelope.Data == nil {
			continue
		}
		for _, account := range envelope.Data.Accounts {
			accounts[account] = append(accounts[account], envelope.Server.ID)
		}
	}
	logger.Debug("accountz/discovered", "duration", time.Since(discoveryStarted))

	entityTimeout := clampEntityTimeout(requestor.Timeout())
	fetchStarted := time.Now()
	var (
		failedServers sync.Map
		mu            sync.Mutex
		wg            sync.WaitGroup
		responses     [][]byte
	)
	jobs := make(chan string)
	workerCount := requestor.ServerCount()
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for account := range jobs {
				serverIDs := accounts[account]
				serverID := ""
				for _, candidate := range serverIDs {
					if _, failed := failedServers.Load(candidate); !failed {
						serverID = candidate
						break
					}
				}
				if serverID == "" {
					serverID = serverIDs[rand.Intn(len(serverIDs))]
				}
				requestCtx, cancel := context.WithTimeout(ctx, entityTimeout)
				response, err := requestor.RequestOne(
					requestCtx,
					fmt.Sprintf("$SYS.REQ.SERVER.%s.ACCOUNTZ", serverID),
					&server.AccountzEventOptions{
						AccountzOptions: server.AccountzOptions{Account: account},
					},
				)
				cancel()
				if err != nil {
					failedServers.Store(serverID, struct{}{})
					logger.Warn("accountz/fetch", "account", account, "error", err)
					continue
				}
				var envelope server.ServerAPIAccountzResponse
				if err := json.Unmarshal(response, &envelope); err != nil || envelope.Error != nil || envelope.Data == nil {
					continue
				}
				mu.Lock()
				responses = append(responses, response)
				mu.Unlock()
			}
		}()
	}
	for account := range accounts {
		jobs <- account
	}
	close(jobs)
	wg.Wait()
	logger.Debug("accountz/fetched", "accounts", len(responses), "duration", time.Since(fetchStarted))
	return responses, nil
}

func Raftz(ctx context.Context, logger *slog.Logger, requestor Requestor, options *RaftzOptions) ([][]byte, error) {
	if options == nil {
		options = &RaftzOptions{}
	}
	discoveryStarted := time.Now()
	inventory, err := requestor.Request(ctx, AccountzSubject, &server.AccountzEventOptions{})
	if err != nil {
		return nil, err
	}
	accounts := make(map[string]struct{})
	for _, response := range inventory {
		if gjson.GetBytes(response, "error").Exists() {
			continue
		}
		for _, account := range gjson.GetBytes(response, "data.accounts").Array() {
			accounts[account.String()] = struct{}{}
		}
	}
	logger.Debug("raftz/discovered", "accounts", len(accounts), "duration", time.Since(discoveryStarted))

	entityTimeout := clampEntityTimeout(requestor.Timeout())
	fetchStarted := time.Now()
	var (
		mu        sync.Mutex
		wg        sync.WaitGroup
		responses [][]byte
	)
	jobs := make(chan string)
	workerCount := requestor.ServerCount()
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for account := range jobs {
				requestCtx, cancel := context.WithTimeout(ctx, entityTimeout)
				accountResponses, err := requestor.RequestAdaptive(
					requestCtx,
					RaftzSubject,
					(&RaftzOptions{Account: account, Group: options.Group}).toServerOptions(),
				)
				cancel()
				if err != nil {
					logger.Warn("raftz/fetch", "account", account, "error", err)
					continue
				}
				for _, response := range accountResponses {
					if gjson.GetBytes(response, "error").Exists() {
						logger.Error(
							"raftz/page",
							"error", gjson.GetBytes(response, "error.description").String(),
							"server", gjson.GetBytes(response, "server.name").String(),
						)
						continue
					}
					if len(gjson.GetBytes(response, "data").Map()) == 0 {
						continue
					}
					mu.Lock()
					responses = append(responses, response)
					mu.Unlock()
				}
			}
		}()
	}
	for account := range accounts {
		jobs <- account
	}
	close(jobs)
	wg.Wait()
	logger.Debug("raftz/fetched", "responses", len(responses), "duration", time.Since(fetchStarted))
	return responses, nil
}

func clampEntityTimeout(timeout time.Duration) time.Duration {
	timeout /= 3
	if timeout < time.Second {
		return time.Second
	}
	if timeout > 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}
