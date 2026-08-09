package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	scraper "github.com/johndoe/nats-scraper"
	"github.com/johndoe/nats-scraper/sink"
	jsSink "github.com/johndoe/nats-scraper/sink/jetstream"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

func TestRunnerStateLayoutMatchesBinary(t *testing.T) {
	var runner Runner
	for name, gotWant := range map[string][2]uintptr{
		"logger":     {unsafe.Offsetof(runner.logger), 0x00},
		"cfg":        {unsafe.Offsetof(runner.cfg), 0x08},
		"nc":         {unsafe.Offsetof(runner.nc), 0x10},
		"sysnc":      {unsafe.Offsetof(runner.sysnc), 0x18},
		"prepare":    {unsafe.Offsetof(runner.prepare), 0x20},
		"publish":    {unsafe.Offsetof(runner.publish), 0x28},
		"epochCount": {unsafe.Offsetof(runner.epochCount), 0x30},
		"microSvc":   {unsafe.Offsetof(runner.microSvc), 0x38},
		"scraping":   {unsafe.Offsetof(runner.scraping), 0x48},
		"scrapingMu": {unsafe.Offsetof(runner.scrapingMu), 0x4c},
		"paused":     {unsafe.Offsetof(runner.paused), 0x54},
		"pausedMu":   {unsafe.Offsetof(runner.pausedMu), 0x58},
	} {
		if gotWant[0] != gotWant[1] {
			t.Errorf("%s offset = %#x, want %#x", name, gotWant[0], gotWant[1])
		}
	}
}

func TestConfigSizeMatchesBinary(t *testing.T) {
	var config Config
	if got := unsafe.Sizeof(config); got != 0x230 {
		t.Fatalf("Config size = %#x, want %#x", got, uintptr(0x230))
	}
	for name, gotWant := range map[string][2]uintptr{
		"Logger":             {unsafe.Offsetof(config.Logger), 0x00},
		"SubjectPrefix":      {unsafe.Offsetof(config.SubjectPrefix), 0x08},
		"StreamName":         {unsafe.Offsetof(config.StreamName), 0x18},
		"ScrapeTimeout":      {unsafe.Offsetof(config.ScrapeTimeout), 0x28},
		"ScrapeInterval":     {unsafe.Offsetof(config.ScrapeInterval), 0x30},
		"SinkType":           {unsafe.Offsetof(config.SinkType), 0x38},
		"SinkBatchSize":      {unsafe.Offsetof(config.SinkBatchSize), 0x48},
		"SinkMaxEpochs":      {unsafe.Offsetof(config.SinkMaxEpochs), 0x50},
		"OnDemandSubject":    {unsafe.Offsetof(config.OnDemandSubject), 0x58},
		"OnDemandQueueGroup": {unsafe.Offsetof(config.OnDemandQueueGroup), 0x68},
		"OnDemandDisabled":   {unsafe.Offsetof(config.OnDemandDisabled), 0x78},
		"IntervalDisabled":   {unsafe.Offsetof(config.IntervalDisabled), 0x79},
		"SinkPrepare":        {unsafe.Offsetof(config.SinkPrepare), 0x80},
		"SinkPublish":        {unsafe.Offsetof(config.SinkPublish), 0x88},
		"ServerFilter":       {unsafe.Offsetof(config.ServerFilter), 0x90},
		"Varz":               {unsafe.Offsetof(config.Varz), 0x98},
		"Connz":              {unsafe.Offsetof(config.Connz), 0xb0},
		"Subsz":              {unsafe.Offsetof(config.Subsz), 0xd0},
		"Routez":             {unsafe.Offsetof(config.Routez), 0x110},
		"Jsz":                {unsafe.Offsetof(config.Jsz), 0x128},
		"Gatewayz":           {unsafe.Offsetof(config.Gatewayz), 0x150},
		"Leafz":              {unsafe.Offsetof(config.Leafz), 0x168},
		"Accountz":           {unsafe.Offsetof(config.Accountz), 0x190},
		"Accstatz":           {unsafe.Offsetof(config.Accstatz), 0x1a8},
		"Healthz":            {unsafe.Offsetof(config.Healthz), 0x1c0},
		"Raftz":              {unsafe.Offsetof(config.Raftz), 0x1d8},
		"Ipqueuesz":          {unsafe.Offsetof(config.Ipqueuesz), 0x208},
	} {
		if gotWant[0] != gotWant[1] {
			t.Errorf("%s offset = %#x, want %#x", name, gotWant[0], gotWant[1])
		}
	}
}

func TestRecoveredCallbackTypesAndInternalTags(t *testing.T) {
	configType := reflect.TypeOf(Config{})
	for fieldName, wantName := range map[string]string{
		"SinkPrepare": "PrepareFunc",
		"SinkPublish": "PublishFunc",
	} {
		field, ok := configType.FieldByName(fieldName)
		if !ok {
			t.Fatalf("missing Config.%s", fieldName)
		}
		if field.Type.PkgPath() != reflect.TypeOf((*sink.PrepareFunc)(nil)).Elem().PkgPath() || field.Type.Name() != wantName {
			t.Errorf("Config.%s type = %s, want sink.%s", fieldName, field.Type, wantName)
		}
	}

	resultType := reflect.TypeOf(scrapeResult{})
	for i, want := range []string{"epoch", "duration", "servers", "-"} {
		field := resultType.Field(i)
		if got := field.Tag.Get("json"); got != want {
			t.Errorf("scrapeResult.%s JSON tag = %q, want %q", field.Name, got, want)
		}
	}
}

func TestDefaultConfigMatchesBinary(t *testing.T) {
	config := DefaultConfig
	if config.Logger != slog.Default() ||
		config.SubjectPrefix != "scrape" ||
		config.StreamName != "scrape" ||
		config.ScrapeTimeout != 30*time.Second ||
		config.ScrapeInterval != 20*time.Second ||
		config.SinkType != "nats" ||
		config.SinkBatchSize != 500 ||
		config.SinkMaxEpochs != 0 ||
		config.OnDemandSubject != "$SCR.ops.trigger" ||
		config.OnDemandQueueGroup != "" ||
		config.OnDemandDisabled ||
		config.IntervalDisabled {
		t.Fatalf("scalar defaults do not match binary: %#v", config)
	}
	for name, frequency := range map[string]int{
		"varz": config.Varz.Frequency, "connz": config.Connz.Frequency,
		"subsz": config.Subsz.Frequency, "routez": config.Routez.Frequency,
		"jsz": config.Jsz.Frequency, "gatewayz": config.Gatewayz.Frequency,
		"leafz": config.Leafz.Frequency, "accountz": config.Accountz.Frequency,
		"accstatz": config.Accstatz.Frequency, "healthz": config.Healthz.Frequency,
		"raftz": config.Raftz.Frequency, "ipqueuesz": config.Ipqueuesz.Frequency,
	} {
		if frequency != 1 {
			t.Errorf("%s frequency = %d, want 1", name, frequency)
		}
	}
	if !config.Connz.Options.Username ||
		!config.Connz.Options.Subscriptions ||
		!config.Connz.Options.SubscriptionsDetail ||
		config.Connz.Options.Limit != 1024 ||
		config.Subsz.Options.Limit != 1024 ||
		config.Routez.Options.Subscriptions ||
		!config.Routez.Options.SubscriptionsDetail ||
		!config.Jsz.Options.Accounts ||
		!config.Jsz.Options.Streams ||
		!config.Jsz.Options.Consumer ||
		!config.Jsz.Options.Config ||
		config.Jsz.Options.Limit != 10000 ||
		!config.Jsz.Options.RaftGroups ||
		!config.Leafz.Options.Subscriptions ||
		!config.Accstatz.Options.IncludeUnused ||
		!config.Healthz.Options.Details ||
		!config.Ipqueuesz.Options.All {
		t.Fatalf("endpoint defaults do not match binary: %#v", config)
	}
}

func TestNewOnlyDefaultsLogger(t *testing.T) {
	config := Config{}
	runner := New(&config, nil, nil)
	if runner.logger != slog.Default() {
		t.Fatal("New did not install slog.Default")
	}
	if runner.cfg != &config || config.Logger != slog.Default() {
		t.Fatal("New did not retain and update the supplied config pointer")
	}
	if runner.nc != nil || runner.sysnc != nil {
		t.Fatal("New changed nil connections")
	}
	if runner.prepare != nil || runner.publish != nil {
		t.Fatal("New resolved sink callbacks before Start")
	}
	if runner.epochCount != 0 {
		t.Fatalf("New epoch count = %d, want zero", runner.epochCount)
	}
	if runner.cfg.SubjectPrefix != "" || runner.cfg.ScrapeTimeout != 0 || runner.cfg.SinkBatchSize != 0 {
		t.Fatalf("New applied non-logger defaults: %#v", runner.cfg)
	}
}

func TestNewUsesDefaultConfigPointerForNilConfig(t *testing.T) {
	runner := New(nil, nil, nil)
	if runner.cfg != &DefaultConfig {
		t.Fatal("New did not use the package DefaultConfig for nil config")
	}
	if runner.logger != DefaultConfig.Logger {
		t.Fatal("New logger does not match DefaultConfig")
	}
}

func TestStartResolvesSinkBeforeDisabledModesCheck(t *testing.T) {
	prepare := func(context.Context, *slog.Logger, *nats.Conn, string, int64, [][]byte, int) (int, error) {
		return 0, nil
	}
	publish := func(context.Context, *slog.Logger, *nats.Conn, string, int64) error {
		return nil
	}

	for _, test := range []struct {
		name      string
		configure func(*Config)
		want      string
	}{
		{
			name: "incomplete custom sink",
			configure: func(config *Config) {
				config.SinkPrepare = prepare
			},
			want: "both SinkPrepare and SinkPublish must be set",
		},
		{
			name: "unknown built-in sink",
			configure: func(config *Config) {
				config.SinkType = "unknown"
			},
			want: `unknown sink type "unknown"`,
		},
		{
			name: "complete custom sink bypasses jetstream assertion",
			configure: func(config *Config) {
				config.SinkType = "jetstream"
				config.SinkPrepare = prepare
				config.SinkPublish = publish
			},
			want: "both interval and on-demand scraping are disabled",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig
			config.Logger = discardLogger()
			config.IntervalDisabled = true
			config.OnDemandDisabled = true
			test.configure(&config)
			runner := New(&config, nil, nil)
			err := runner.Start(context.Background())
			if err == nil || err.Error() != test.want {
				t.Fatalf("Start error = %v, want %q", err, test.want)
			}
			if test.name == "complete custom sink bypasses jetstream assertion" && (runner.prepare == nil || runner.publish == nil) {
				t.Fatal("custom sink callbacks were not installed before the mode check")
			}
		})
	}
}

func TestDoScrapeSkipsBeforeAllocatingEpoch(t *testing.T) {
	config := DefaultConfig
	runner := &Runner{logger: discardLogger(), cfg: &config}
	runner.scraping = true
	runner.Pause()

	result, executed := runner.doScrape(context.Background(), "test")
	if executed || result != nil || runner.epochCount != 0 {
		t.Fatalf("paused scrape = (%#v, %t), epoch count %d", result, executed, runner.epochCount)
	}

	runner.Resume()
	result, executed = runner.doScrape(context.Background(), "test")
	if executed || result != nil || runner.epochCount != 0 {
		t.Fatalf("busy scrape = (%#v, %t), epoch count %d", result, executed, runner.epochCount)
	}
}

func TestDoScrapeTreatsHandlerAndMarkerErrorsAsLoggingOnly(t *testing.T) {
	nc := runRunnerNATSServer(t)
	installSystemResponder(t, nc, false)

	cfg := runnerTestConfig()
	var mu sync.Mutex
	var published, prepared []string
	cfg.SinkPublish = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, subject string, _ int64) error {
		mu.Lock()
		published = append(published, subject)
		mu.Unlock()
		return errors.New("marker failure")
	}
	cfg.SinkPrepare = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, subject string, _ int64, _ [][]byte, _ int) (int, error) {
		mu.Lock()
		prepared = append(prepared, subject)
		mu.Unlock()
		return 0, errors.New("prepare failure")
	}
	runner := New(&cfg, nc, nc)
	runner.prepare = cfg.SinkPrepare
	runner.publish = cfg.SinkPublish

	result, executed := runner.doScrape(context.Background(), "test")
	if !executed || result == nil {
		t.Fatalf("scrape = (%#v, %t), want executed result", result, executed)
	}
	if result.Error != nil || result.Servers != 1 || result.Duration <= 0 {
		t.Fatalf("result = %#v, want successful one-server scrape", result)
	}
	if len(published) != 2 || !strings.HasSuffix(published[0], ".start") || !strings.HasSuffix(published[1], ".end") {
		t.Fatalf("published subjects = %v", published)
	}
	if len(prepared) != 1 || !strings.HasSuffix(prepared[0], ".servers") {
		t.Fatalf("prepared subjects = %v", prepared)
	}
}

func TestDoScrapeSkipsEveryDisabledEndpoint(t *testing.T) {
	nc := runRunnerNATSServer(t)
	installSystemResponder(t, nc, true)

	cfg := runnerTestConfig()
	cfg.Varz.Disabled = true
	cfg.Connz.Disabled = true
	cfg.Subsz.Disabled = true
	cfg.Routez.Disabled = true
	cfg.Jsz.Disabled = true
	cfg.Gatewayz.Disabled = true
	cfg.Leafz.Disabled = true
	cfg.Accountz.Disabled = true
	cfg.Accstatz.Disabled = true
	cfg.Healthz.Disabled = true
	cfg.Raftz.Disabled = true
	cfg.Ipqueuesz.Disabled = true
	for _, frequency := range []*int{
		&cfg.Varz.Frequency, &cfg.Connz.Frequency, &cfg.Subsz.Frequency,
		&cfg.Routez.Frequency, &cfg.Jsz.Frequency, &cfg.Gatewayz.Frequency,
		&cfg.Leafz.Frequency, &cfg.Accountz.Frequency, &cfg.Accstatz.Frequency,
		&cfg.Healthz.Frequency, &cfg.Raftz.Frequency, &cfg.Ipqueuesz.Frequency,
	} {
		*frequency = 1
	}
	var prepared []string
	cfg.SinkPublish = func(context.Context, *slog.Logger, *nats.Conn, string, int64) error { return nil }
	cfg.SinkPrepare = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, subject string, _ int64, payloads [][]byte, _ int) (int, error) {
		prepared = append(prepared, subject)
		return len(payloads), nil
	}
	runner := New(&cfg, nc, nc)
	runner.prepare = cfg.SinkPrepare
	runner.publish = cfg.SinkPublish

	result, executed := runner.doScrape(context.Background(), "test")
	if !executed || result == nil || result.Error != nil {
		t.Fatalf("scrape = (%#v, %t)", result, executed)
	}
	if len(prepared) != 1 || !strings.HasSuffix(prepared[0], ".servers") {
		t.Fatalf("prepared subjects = %v, want only servers", prepared)
	}
}

func TestEndpointHandlersReturnPrepareTuple(t *testing.T) {
	handlers := []func(*Runner, context.Context, scraper.Requestor, int64) (int, error){
		(*Runner).handleServers,
		(*Runner).handleVarz,
		(*Runner).handleConnz,
		(*Runner).handleJsz,
		(*Runner).handleRoutez,
		(*Runner).handleSubsz,
		(*Runner).handleGatewayz,
		(*Runner).handleLeafz,
		(*Runner).handleAccountz,
		(*Runner).handleAccstatz,
		(*Runner).handleHealthz,
		(*Runner).handleRaftz,
		(*Runner).handleIpqueuesz,
	}
	if len(handlers) != 13 {
		t.Fatalf("handler count = %d, want 13", len(handlers))
	}

	nc := runRunnerNATSServer(t)
	installSystemResponder(t, nc, true)
	cfg := runnerTestConfig()
	cfg.Varz.Disabled = false
	wantErr := errors.New("prepare sentinel")
	cfg.SinkPrepare = func(context.Context, *slog.Logger, *nats.Conn, string, int64, [][]byte, int) (int, error) {
		return 37, wantErr
	}
	runner := New(&cfg, nc, nc)
	runner.prepare = cfg.SinkPrepare
	requestor := scraper.NewRequestor([]string{"S1"}, nc, cfg.ScrapeTimeout, cfg.Logger, 0)

	count, err := runner.handleVarz(context.Background(), requestor, 123)
	if count != 37 || !errors.Is(err, wantErr) {
		t.Fatalf("handleVarz returned (%d, %v), want (37, sentinel)", count, err)
	}
}

func TestDoScrapeDistinguishesNoActiveServersAndAllExcluded(t *testing.T) {
	for _, test := range []struct {
		name      string
		responder bool
		filter    func(scraper.ServerInfo) bool
		want      string
	}{
		{name: "no active servers", want: "no active servers"},
		{
			name:      "all active servers excluded",
			responder: true,
			filter:    func(scraper.ServerInfo) bool { return false },
			want:      "all active servers excluded by filter",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			nc := runRunnerNATSServer(t)
			if test.responder {
				installSystemResponder(t, nc, false)
			} else {
				subscription, err := nc.Subscribe("$SYS.REQ.SERVER.PING", func(*nats.Msg) {})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = subscription.Unsubscribe() })
				if err := nc.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			cfg := runnerTestConfig()
			cfg.ServerFilter = test.filter
			published := 0
			cfg.SinkPublish = func(context.Context, *slog.Logger, *nats.Conn, string, int64) error {
				published++
				return nil
			}
			cfg.SinkPrepare = func(context.Context, *slog.Logger, *nats.Conn, string, int64, [][]byte, int) (int, error) {
				t.Fatal("prepare called before scrape admission")
				return 0, nil
			}
			runner := New(&cfg, nc, nc)

			result, executed := runner.doScrape(context.Background(), "test")
			if !executed || result == nil || result.Error == nil || result.Error.Error() != test.want {
				t.Fatalf("scrape = (%#v, %t), want error %q", result, executed, test.want)
			}
			if published != 0 || runner.epochCount != 1 || result.Duration != 0 || result.Servers != 0 {
				t.Fatalf("published=%d count=%d duration=%v servers=%d", published, runner.epochCount, result.Duration, result.Servers)
			}
		})
	}
}

func TestRunEndpointZeroFrequencyPanicsEvenWhenDisabled(t *testing.T) {
	nc := runRunnerNATSServer(t)
	installSystemResponder(t, nc, false)
	cfg := runnerTestConfig()
	cfg.Varz.Disabled = true
	cfg.Varz.Frequency = 0
	cfg.SinkPrepare = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, _ string, _ int64, payloads [][]byte, _ int) (int, error) {
		return len(payloads), nil
	}
	cfg.SinkPublish = func(context.Context, *slog.Logger, *nats.Conn, string, int64) error { return nil }
	runner := New(&cfg, nc, nc)
	runner.prepare = cfg.SinkPrepare
	runner.publish = cfg.SinkPublish
	defer func() {
		if recover() == nil {
			t.Fatal("zero frequency did not panic")
		}
	}()
	runner.doScrape(context.Background(), "test")
}

func TestStartResumesIntervalFromPersistedEndEpoch(t *testing.T) {
	nc := runRunnerJetStreamNATSServer(t)
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer setupCancel()
	js, err := jsapi.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(setupCtx, jsapi.StreamConfig{
		Name:     "scrape",
		Subjects: []string{"scrape.>"},
		Storage:  jsapi.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	epoch := time.Now().Unix()
	if err := jsSink.Publish(
		setupCtx,
		slog.Default(),
		nc,
		"scrape."+strconv.FormatInt(epoch, 10)+".end",
		epoch,
	); err != nil {
		t.Fatal(err)
	}

	pingSeen := make(chan time.Time, 2)
	subscription, err := nc.Subscribe("$SYS.REQ.SERVER.PING", func(request *nats.Msg) {
		pingSeen <- time.Now()
		_ = request.Respond([]byte(`{"server":{"id":"S1","name":"n1","cluster":"c1"},"statsz":{}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subscription.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	cfg := runnerTestConfig()
	cfg.SinkType = "jetstream"
	cfg.ScrapeInterval = 3 * time.Second
	cfg.OnDemandDisabled = true
	cfg.SinkPrepare = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, _ string, _ int64, payloads [][]byte, _ int) (int, error) {
		return len(payloads), nil
	}
	cfg.SinkPublish = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, subject string, _ int64) error {
		if strings.HasSuffix(subject, ".end") {
			cancelRun()
		}
		return nil
	}
	runner := New(&cfg, nc, nc)

	expectedDelay := cfg.ScrapeInterval - time.Since(time.Unix(epoch, 0))
	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- runner.Start(runCtx) }()

	var firstPing time.Time
	select {
	case firstPing = <-pingSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not resume scraping from persisted epoch")
	}
	elapsed := firstPing.Sub(started)
	if elapsed < expectedDelay-200*time.Millisecond || elapsed > expectedDelay+750*time.Millisecond {
		t.Fatalf("first discovery after %v, want persisted-epoch delay near %v", elapsed, expectedDelay)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not finish after the resumed scrape")
	}
}

func TestOnDemandControlEndpoints(t *testing.T) {
	nc := runRunnerNATSServer(t)
	cfg := runnerTestConfig()
	runner := New(&cfg, nc, nc)
	if err := runner.setupOnDemandService(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.microSvc.Stop() })

	info := runner.microSvc.Info()
	if info.Name != "scraper" || info.Version != "1.0.0" || info.Description != "On-demand NATS server scraping" {
		t.Fatalf("service info = %#v", info)
	}
	wantEndpoints := map[string]string{
		"scrape": cfg.OnDemandSubject,
		"pause":  "$SCR.ops.pause",
		"resume": "$SCR.ops.resume",
		"status": "$SCR.ops.status",
	}
	for _, endpoint := range info.Endpoints {
		if wantEndpoints[endpoint.Name] != endpoint.Subject {
			t.Fatalf("endpoint %q subject = %q", endpoint.Name, endpoint.Subject)
		}
		delete(wantEndpoints, endpoint.Name)
	}
	if len(wantEndpoints) != 0 {
		t.Fatalf("missing endpoints: %v", wantEndpoints)
	}

	request := func(subject, want string) {
		t.Helper()
		response, err := nc.Request(subject, nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if string(response.Data) != want {
			t.Fatalf("%s response = %q, want %q", subject, response.Data, want)
		}
	}
	request("$SCR.ops.pause", `{"paused":true}`)
	request("$SCR.ops.status", `{"paused":true}`)
	request("$SCR.ops.trigger", `{"executed":false,"reason":"scrape already in progress"}`)
	request("$SCR.ops.resume", `{"paused":false}`)
	request("$SCR.ops.status", `{"paused":false}`)
}

func TestOnDemandTriggerResponseSemantics(t *testing.T) {
	t.Run("successful scrape", func(t *testing.T) {
		nc := runRunnerNATSServer(t)
		installSystemResponder(t, nc, false)
		cfg := runnerTestConfig()
		runner := New(&cfg, nc, nc)
		runner.prepare = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, _ string, _ int64, payloads [][]byte, _ int) (int, error) {
			return len(payloads), nil
		}
		runner.publish = func(context.Context, *slog.Logger, *nats.Conn, string, int64) error { return nil }
		if err := runner.setupOnDemandService(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runner.microSvc.Stop() })

		response, err := nc.Request(cfg.OnDemandSubject, nil, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var decoded onDemandResponse
		if err := json.Unmarshal(response.Data, &decoded); err != nil {
			t.Fatal(err)
		}
		if !decoded.Executed || decoded.Epoch == 0 || decoded.Duration == "" || decoded.Duration == "0s" || decoded.Servers != 1 || decoded.Reason != "" {
			t.Fatalf("response = %#v", decoded)
		}
		want := fmt.Sprintf(
			`{"executed":true,"epoch":%d,"duration":%q,"servers":1}`,
			decoded.Epoch,
			decoded.Duration,
		)
		if string(response.Data) != want {
			t.Fatalf("response = %q, want %q", response.Data, want)
		}
	})

	t.Run("discovery failure", func(t *testing.T) {
		nc := runRunnerNATSServer(t)
		cfg := runnerTestConfig()
		runner := New(&cfg, nc, nc)
		runner.prepare = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, _ string, _ int64, payloads [][]byte, _ int) (int, error) {
			return len(payloads), nil
		}
		runner.publish = func(context.Context, *slog.Logger, *nats.Conn, string, int64) error { return nil }
		if err := runner.setupOnDemandService(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runner.microSvc.Stop() })

		response, err := nc.Request(cfg.OnDemandSubject, nil, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var decoded onDemandResponse
		if err := json.Unmarshal(response.Data, &decoded); err != nil {
			t.Fatal(err)
		}
		if !decoded.Executed || decoded.Epoch == 0 || decoded.Duration != "0s" || decoded.Servers != 0 || decoded.Reason != "" {
			t.Fatalf("response = %#v", decoded)
		}
		want := fmt.Sprintf(`{"executed":true,"epoch":%d,"duration":"0s"}`, decoded.Epoch)
		if string(response.Data) != want {
			t.Fatalf("response = %q, want %q", response.Data, want)
		}
	})
}

func TestOnDemandControlSubjectsWithoutDot(t *testing.T) {
	nc := runRunnerNATSServer(t)
	cfg := runnerTestConfig()
	cfg.OnDemandSubject = "trigger"
	runner := New(&cfg, nc, nc)
	if err := runner.setupOnDemandService(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.microSvc.Stop() })

	want := map[string]string{
		"scrape": "trigger",
		"pause":  "trigger.pause",
		"resume": "trigger.resume",
		"status": "trigger.status",
	}
	for _, endpoint := range runner.microSvc.Info().Endpoints {
		if endpoint.Subject != want[endpoint.Name] {
			t.Fatalf("endpoint %q subject = %q, want %q", endpoint.Name, endpoint.Subject, want[endpoint.Name])
		}
		delete(want, endpoint.Name)
	}
	if len(want) != 0 {
		t.Fatalf("missing endpoints = %v", want)
	}
}

func TestOnDemandDisabledSkipsSetupWithoutConnection(t *testing.T) {
	cfg := runnerTestConfig()
	cfg.OnDemandDisabled = true
	runner := New(&cfg, nil, nil)
	if err := runner.setupOnDemandService(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runner.microSvc != nil {
		t.Fatalf("service = %#v, want nil", runner.microSvc)
	}
}

func TestOnDemandTriggerIsRejectedDuringAdmittedScrape(t *testing.T) {
	nc := runRunnerNATSServer(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var pings atomic.Int32
	subscription, err := nc.Subscribe("$SYS.REQ.SERVER.PING", func(request *nats.Msg) {
		if pings.Add(1) == 1 {
			close(started)
			<-release
		}
		_ = request.Respond([]byte(`{"server":{"id":"S1","name":"n1","cluster":"c1"},"statsz":{}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subscription.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	cfg := runnerTestConfig()
	runner := New(&cfg, nc, nc)
	runner.prepare = func(_ context.Context, _ *slog.Logger, _ *nats.Conn, _ string, _ int64, payloads [][]byte, _ int) (int, error) {
		return len(payloads), nil
	}
	runner.publish = func(context.Context, *slog.Logger, *nats.Conn, string, int64) error { return nil }
	if err := runner.setupOnDemandService(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.microSvc.Stop() })

	type scrapeCall struct {
		result   *scrapeResult
		executed bool
	}
	completed := make(chan scrapeCall, 1)
	go func() {
		result, executed := runner.doScrape(context.Background(), "interval")
		completed <- scrapeCall{result: result, executed: executed}
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("admitted scrape did not reach discovery")
	}
	response, err := nc.Request(cfg.OnDemandSubject, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(response.Data), `{"executed":false,"reason":"scrape already in progress"}`; got != want {
		t.Fatalf("overlapping on-demand response = %q, want %q", got, want)
	}
	close(release)

	select {
	case call := <-completed:
		if !call.executed || call.result == nil || call.result.Error != nil || call.result.Servers != 1 {
			t.Fatalf("admitted scrape = (%#v, %t)", call.result, call.executed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admitted scrape did not finish")
	}
	if runner.epochCount != 1 {
		t.Fatalf("epoch count = %d, want only the admitted scrape", runner.epochCount)
	}
}

func TestOnDemandSubjectCannotOverlapJetStreamSink(t *testing.T) {
	cfg := runnerTestConfig()
	cfg.SinkType = "jetstream"
	cfg.OnDemandSubject = "scrape.control"
	runner := &Runner{logger: cfg.Logger, cfg: &cfg}
	err := runner.setupOnDemandService(context.Background())
	want := `on-demand subject "scrape.control" is within the sink stream subject space "scrape".> ; choose a subject outside it (e.g. the default $SCR.ops.trigger)`
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func runnerTestConfig() Config {
	cfg := DefaultConfig
	cfg.Logger = discardLogger()
	cfg.SinkType = "noop"
	cfg.ScrapeTimeout = 25 * time.Millisecond
	cfg.ScrapeInterval = time.Second
	cfg.Varz.Frequency = 2
	cfg.Connz.Frequency = 2
	cfg.Subsz.Frequency = 2
	cfg.Routez.Frequency = 2
	cfg.Jsz.Frequency = 2
	cfg.Gatewayz.Frequency = 2
	cfg.Leafz.Frequency = 2
	cfg.Accountz.Frequency = 2
	cfg.Accstatz.Frequency = 2
	cfg.Healthz.Frequency = 2
	cfg.Raftz.Frequency = 2
	cfg.Ipqueuesz.Frequency = 2
	return cfg
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func installSystemResponder(t *testing.T, nc *nats.Conn, includeVarz bool) {
	t.Helper()
	response := []byte(`{"server":{"id":"S1","name":"n1","cluster":"c1"},"data":{}}`)
	subject := "$SYS.REQ.SERVER.PING"
	if includeVarz {
		subject = "$SYS.>"
	}
	sub, err := nc.Subscribe(subject, func(message *nats.Msg) {
		_ = message.Respond(response)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
}

func runRunnerNATSServer(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:            "127.0.0.1",
		Port:            -1,
		NoLog:           true,
		NoSigs:          true,
		NoSystemAccount: true,
	})
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

func runRunnerJetStreamNATSServer(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:            "127.0.0.1",
		Port:            -1,
		NoLog:           true,
		NoSigs:          true,
		NoSystemAccount: true,
		JetStream:       true,
		StoreDir:        t.TempDir(),
	})
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
