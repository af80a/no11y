package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/johndoe/nats-scraper"
	"github.com/johndoe/nats-scraper/sink"
	jsSink "github.com/johndoe/nats-scraper/sink/jetstream"
	natsSink "github.com/johndoe/nats-scraper/sink/nats"
	noopSink "github.com/johndoe/nats-scraper/sink/noop"
	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

type scrapeResult struct {
	Epoch    int64         `json:"epoch"`
	Duration time.Duration `json:"duration"`
	Servers  int           `json:"servers"`
	Error    error         `json:"-"`
}

type onDemandResponse struct {
	Executed bool   `json:"executed"`
	Epoch    int64  `json:"epoch,omitempty"`
	Duration string `json:"duration,omitempty"`
	Servers  int    `json:"servers,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

type Runner struct {
	logger *slog.Logger
	cfg    *Config
	nc     *nats.Conn
	sysnc  *nats.Conn

	prepare sink.PrepareFunc
	publish sink.PublishFunc

	epochCount int
	microSvc   micro.Service

	scraping   bool
	scrapingMu sync.Mutex
	paused     bool
	pausedMu   sync.Mutex
}

func New(config *Config, sinkConnection, systemConnection *nats.Conn) *Runner {
	if config == nil {
		config = &DefaultConfig
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Runner{
		logger: config.Logger,
		cfg:    config,
		nc:     sinkConnection,
		sysnc:  systemConnection,
	}
}

func (r *Runner) Pause() {
	r.pausedMu.Lock()
	r.paused = true
	r.pausedMu.Unlock()
}

func (r *Runner) Resume() {
	r.pausedMu.Lock()
	r.paused = false
	r.pausedMu.Unlock()
}

func (r *Runner) IsPaused() bool {
	r.pausedMu.Lock()
	defer r.pausedMu.Unlock()
	return r.paused
}

func (r *Runner) setupOnDemandService(ctx context.Context) error {
	if r.cfg.OnDemandDisabled {
		return nil
	}
	if r.cfg.SinkType == "jetstream" && strings.HasPrefix(r.cfg.OnDemandSubject, r.cfg.SubjectPrefix+".") {
		return fmt.Errorf(
			"on-demand subject %q is within the sink stream subject space %q.> ; choose a subject outside it (e.g. the default $SCR.ops.trigger)",
			r.cfg.OnDemandSubject,
			r.cfg.SubjectPrefix,
		)
	}

	service, err := micro.AddService(r.nc, micro.Config{
		Name:        "scraper",
		Version:     "1.0.0",
		Description: "On-demand NATS server scraping",
		QueueGroup:  r.cfg.OnDemandQueueGroup,
	})
	if err != nil {
		return fmt.Errorf("failed to create on-demand service: %w", err)
	}

	base := r.cfg.OnDemandSubject
	if index := strings.LastIndex(base, "."); index > 0 {
		base = base[:index]
	}
	if err := service.AddEndpoint(
		"scrape",
		micro.ContextHandler(ctx, r.handleOnDemandRequest),
		micro.WithEndpointSubject(r.cfg.OnDemandSubject),
	); err != nil {
		service.Stop()
		return fmt.Errorf("failed to add scrape endpoint: %w", err)
	}
	if err := service.AddEndpoint("pause", micro.HandlerFunc(func(request micro.Request) {
		r.Pause()
		_ = request.Respond([]byte(`{"paused":true}`))
	}), micro.WithEndpointSubject(base+".pause")); err != nil {
		service.Stop()
		return fmt.Errorf("failed to add pause endpoint: %w", err)
	}
	if err := service.AddEndpoint("resume", micro.HandlerFunc(func(request micro.Request) {
		r.Resume()
		_ = request.Respond([]byte(`{"paused":false}`))
	}), micro.WithEndpointSubject(base+".resume")); err != nil {
		service.Stop()
		return fmt.Errorf("failed to add resume endpoint: %w", err)
	}
	if err := service.AddEndpoint("status", micro.HandlerFunc(func(request micro.Request) {
		response, err := json.Marshal(map[string]bool{"paused": r.IsPaused()})
		if err != nil {
			_ = request.Error("500", err.Error(), nil)
			return
		}
		_ = request.Respond(response)
	}), micro.WithEndpointSubject(base+".status")); err != nil {
		service.Stop()
		return fmt.Errorf("failed to add status endpoint: %w", err)
	}
	r.microSvc = service
	r.logger.Debug("on-demand service created", "subject", r.cfg.OnDemandSubject, "queue_group", r.cfg.OnDemandQueueGroup)
	return nil
}

func (r *Runner) doScrape(ctx context.Context, trigger string) (*scrapeResult, bool) {
	r.scrapingMu.Lock()
	if r.IsPaused() {
		r.scrapingMu.Unlock()
		r.logger.Debug("scrape skipped, paused", "trigger", trigger)
		return nil, false
	}
	if r.scraping {
		r.scrapingMu.Unlock()
		r.logger.Debug("scrape skipped, already in progress", "trigger", trigger)
		return nil, false
	}
	r.scraping = true
	r.scrapingMu.Unlock()
	defer func() {
		r.scrapingMu.Lock()
		r.scraping = false
		r.scrapingMu.Unlock()
	}()

	started := time.Now()
	epoch := started.Unix()
	r.epochCount++

	active, err := scraper.CurrentActiveServerInfos(ctx, r.logger, r.sysnc, r.cfg.ScrapeTimeout)
	if err != nil {
		r.logger.Warn("scrape", "trigger", trigger, "err", err)
		return &scrapeResult{Epoch: epoch, Error: err}, true
	}

	included := make([]scraper.ServerInfo, 0, len(active))
	excluded := 0
	for _, info := range active {
		if r.cfg.ServerFilter != nil && !r.cfg.ServerFilter(info) {
			excluded++
			r.logger.Debug("scrape/excluded", "server", info.Name, "cluster", info.Cluster, "id", info.ID)
			continue
		}
		included = append(included, info)
	}
	if len(included) == 0 {
		var err error
		if excluded > 0 {
			err = errors.New("all active servers excluded by filter")
		} else {
			err = errors.New("no active servers")
		}
		r.logger.Warn("scrape/start", "trigger", trigger, "err", err)
		return &scrapeResult{Epoch: epoch, Error: err}, true
	}

	ids := make([]string, 0, len(included))
	for _, info := range included {
		ids = append(ids, info.ID)
	}
	requestor := scraper.NewRequestor(ids, r.sysnc, r.cfg.ScrapeTimeout, r.logger, excluded)
	r.logger.Debug("scrape/start",
		"trigger", trigger,
		"epoch", epoch,
		"interval", r.cfg.ScrapeInterval,
		"servers", len(included),
		"excluded", excluded)

	if err := r.publish(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.start", r.cfg.SubjectPrefix, epoch), epoch); err != nil {
		r.logger.Error("scrape/start/error", "err", err)
	}

	if _, err := r.handleServers(ctx, requestor, epoch); err != nil {
		r.logger.Error("scrape/servers/error", "err", err)
	}
	if r.epochCount%r.cfg.Varz.Frequency == 0 {
		if _, err := r.handleVarz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/varz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Accountz.Frequency == 0 {
		if _, err := r.handleAccountz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/accountz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Connz.Frequency == 0 {
		if _, err := r.handleConnz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/connz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Jsz.Frequency == 0 {
		if _, err := r.handleJsz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/jsz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Routez.Frequency == 0 {
		if _, err := r.handleRoutez(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/routez/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Subsz.Frequency == 0 {
		if _, err := r.handleSubsz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/subsz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Gatewayz.Frequency == 0 {
		if _, err := r.handleGatewayz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/gatewayz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Leafz.Frequency == 0 {
		if _, err := r.handleLeafz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/leafz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Accstatz.Frequency == 0 {
		if _, err := r.handleAccstatz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/accstatz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Healthz.Frequency == 0 {
		if _, err := r.handleHealthz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/healthz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Raftz.Frequency == 0 {
		if _, err := r.handleRaftz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/raftz/error", "err", err)
		}
	}
	if r.epochCount%r.cfg.Ipqueuesz.Frequency == 0 {
		if _, err := r.handleIpqueuesz(ctx, requestor, epoch); err != nil {
			r.logger.Error("scrape/ipqueuesz/error", "err", err)
		}
	}

	if err := r.publish(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.end", r.cfg.SubjectPrefix, epoch), epoch); err != nil {
		r.logger.Error("scrape/end/error", "err", err)
	}
	duration := time.Since(started)
	r.logger.Debug("scrape/end", "trigger", trigger, "epoch", epoch, "duration", duration)
	return &scrapeResult{Epoch: epoch, Duration: duration, Servers: len(included)}, true
}

func (r *Runner) handleOnDemandRequest(ctx context.Context, request micro.Request) {
	result, executed := r.doScrape(ctx, "on-demand")
	response := onDemandResponse{Executed: executed}
	if executed && result != nil {
		response.Epoch = result.Epoch
		response.Duration = result.Duration.String()
		response.Servers = result.Servers
	} else {
		response.Reason = "scrape already in progress"
	}
	if err := request.RespondJSON(response); err != nil {
		r.logger.Error("on-demand response error", "err", err)
	}
}

func (r *Runner) assertStreamConfig(ctx context.Context) error {
	js, err := jsapi.New(r.nc)
	if err != nil {
		return err
	}
	stream, err := js.Stream(ctx, r.cfg.StreamName)
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	expected := r.cfg.SubjectPrefix + ".>"
	if len(info.Config.Subjects) != 1 {
		return fmt.Errorf("stream %q should have exactly one subject", r.cfg.StreamName)
	}
	if info.Config.Subjects[0] != expected {
		return fmt.Errorf("stream %q should have subject %q", r.cfg.StreamName, expected)
	}
	return nil
}

func (r *Runner) Start(ctx context.Context) error {
	if r.cfg.SinkPrepare != nil || r.cfg.SinkPublish != nil {
		if r.cfg.SinkPrepare == nil || r.cfg.SinkPublish == nil {
			return errors.New("both SinkPrepare and SinkPublish must be set")
		}
		r.prepare = r.cfg.SinkPrepare
		r.publish = r.cfg.SinkPublish
	} else {
		switch r.cfg.SinkType {
		case "jetstream":
			if err := r.assertStreamConfig(ctx); err != nil {
				return err
			}
			r.prepare = jsSink.Prepare
			r.publish = jsSink.Publish
		case "nats":
			r.prepare = natsSink.Prepare
			r.publish = natsSink.Publish
		case "noop":
			r.prepare = noopSink.Prepare[[]byte]
			r.publish = noopSink.Publish
		default:
			return fmt.Errorf("unknown sink type %q", r.cfg.SinkType)
		}
	}
	if r.cfg.IntervalDisabled && r.cfg.OnDemandDisabled {
		return errors.New("both interval and on-demand scraping are disabled")
	}
	if err := r.setupOnDemandService(ctx); err != nil {
		return err
	}
	defer func() {
		if r.microSvc != nil {
			r.microSvc.Stop()
		}
	}()
	if r.cfg.IntervalDisabled {
		<-ctx.Done()
		return nil
	}

	delay := time.Duration(0)
	if r.cfg.SinkType == "jetstream" {
		epoch, err := jsSink.LastEpoch(ctx, r.nc, r.cfg.StreamName, r.cfg.SubjectPrefix)
		if err != nil {
			r.logger.Debug("failed to read last epoch, scraping immediately", "err", err)
		} else if epoch > 0 {
			elapsed := time.Since(time.Unix(epoch, 0))
			if elapsed < r.cfg.ScrapeInterval {
				delay = r.cfg.ScrapeInterval - elapsed
				r.logger.Debug("resuming scrape interval from last epoch", "delay", delay, "last_epoch", epoch)
			}
		}
	}

	r.pruneEpochs(ctx)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			r.pruneEpochs(ctx)
			result, executed := r.doScrape(ctx, "interval")
			next := r.cfg.ScrapeInterval
			if executed && result != nil && result.Error == nil {
				if result.Duration > r.cfg.ScrapeInterval {
					r.logger.Warn("scrape took longer than configured interval",
						"interval", r.cfg.ScrapeInterval.Seconds(),
						"duration", result.Duration.Seconds())
				} else {
					next -= result.Duration
					r.logger.Debug("new scrape interval", "duration", next.Seconds())
				}
			}
			timer.Reset(next)
		}
	}
}

func (r *Runner) pruneEpochs(ctx context.Context) {
	if r.cfg.SinkType != "jetstream" || r.cfg.SinkMaxEpochs <= 0 {
		return
	}
	r.logger.Debug("prune/start", "max_epochs", r.cfg.SinkMaxEpochs)
	started := time.Now()
	pruned, err := jsSink.Prune(ctx, r.nc, r.cfg.StreamName, r.cfg.SubjectPrefix, r.cfg.SinkMaxEpochs)
	if err != nil {
		r.logger.Error("prune/error", "err", err)
		return
	}
	r.logger.Debug("prune/end", "num_pruned", pruned, "duration", time.Since(started))
}

func (r *Runner) handleServers(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	payloads, err := scraper.Servers(ctx, requestor)
	if err != nil {
		r.logger.Error("scrape/servers/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.servers", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleVarz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Varz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Varz(ctx, r.logger, requestor, &r.cfg.Varz.Options)
	if err != nil {
		r.logger.Error("scrape/varz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.varz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleConnz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Connz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Connz(ctx, r.logger, requestor, &r.cfg.Connz.Options)
	if err != nil {
		r.logger.Error("scrape/connz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.connz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleJsz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Jsz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Jsz(ctx, r.logger, requestor, &r.cfg.Jsz.Options)
	if err != nil {
		r.logger.Error("scrape/jsz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.jsz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleRoutez(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Routez.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Routez(ctx, r.logger, requestor, &r.cfg.Routez.Options)
	if err != nil {
		r.logger.Error("scrape/routez/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.routez", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleSubsz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Subsz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Subsz(ctx, r.logger, requestor, &r.cfg.Subsz.Options)
	if err != nil {
		r.logger.Error("scrape/subsz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.subsz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleGatewayz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Gatewayz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Gatewayz(ctx, r.logger, requestor, &r.cfg.Gatewayz.Options)
	if err != nil {
		r.logger.Error("scrape/gatewayz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.gatewayz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleLeafz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Leafz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Leafz(ctx, r.logger, requestor, &r.cfg.Leafz.Options)
	if err != nil {
		r.logger.Error("scrape/leafz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.leafz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleAccountz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Accountz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Accountz(ctx, r.logger, requestor, &r.cfg.Accountz.Options)
	if err != nil {
		r.logger.Error("scrape/accountz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.accountz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleAccstatz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Accstatz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Accstatz(ctx, r.logger, requestor, &r.cfg.Accstatz.Options)
	if err != nil {
		r.logger.Error("scrape/accstatz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.accstatz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleHealthz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Healthz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Healthz(ctx, r.logger, requestor, &r.cfg.Healthz.Options)
	if err != nil {
		r.logger.Error("scrape/healthz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.healthz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleRaftz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Raftz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Raftz(ctx, r.logger, requestor, &r.cfg.Raftz.Options)
	if err != nil {
		r.logger.Error("scrape/raftz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.raftz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}

func (r *Runner) handleIpqueuesz(ctx context.Context, requestor scraper.Requestor, epoch int64) (int, error) {
	if r.cfg.Ipqueuesz.Disabled {
		return 0, nil
	}
	payloads, err := scraper.Ipqueuesz(ctx, r.logger, requestor, &r.cfg.Ipqueuesz.Options)
	if err != nil {
		r.logger.Error("scrape/ipqueuesz/error", "err", err)
	}
	return r.prepare(ctx, r.logger, r.nc, fmt.Sprintf("%s.%d.ipqueuesz", r.cfg.SubjectPrefix, epoch), epoch, payloads, r.cfg.SinkBatchSize)
}
