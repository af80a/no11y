package runner

import (
	"log/slog"
	"time"

	"github.com/johndoe/nats-scraper"
	"github.com/johndoe/nats-scraper/sink"
)

type Endpoint[T any] struct {
	Disabled  bool `json:"disabled"`
	Frequency int  `json:"frequency"`
	Options   T    `json:"options"`
}

type Config struct {
	Logger             *slog.Logger                  `json:"-"`
	SubjectPrefix      string                        `json:"prefix"`
	StreamName         string                        `json:"stream"`
	ScrapeTimeout      time.Duration                 `json:"scrape_timeout"`
	ScrapeInterval     time.Duration                 `json:"scrape_interval"`
	SinkType           string                        `json:"sink_type"`
	SinkBatchSize      int                           `json:"sink_batch_size"`
	SinkMaxEpochs      int                           `json:"sink_max_epochs"`
	OnDemandSubject    string                        `json:"on_demand_subject"`
	OnDemandQueueGroup string                        `json:"on_demand_queue_group"`
	OnDemandDisabled   bool                          `json:"on_demand_disabled"`
	IntervalDisabled   bool                          `json:"interval_disabled"`
	SinkPrepare        sink.PrepareFunc              `json:"-"`
	SinkPublish        sink.PublishFunc              `json:"-"`
	ServerFilter       func(scraper.ServerInfo) bool `json:"-"`

	Varz      Endpoint[scraper.VarzOptions]         `json:"varz"`
	Connz     Endpoint[scraper.ConnzOptions]        `json:"connz"`
	Subsz     Endpoint[scraper.SubszOptions]        `json:"subsz"`
	Routez    Endpoint[scraper.RoutezOptions]       `json:"routez"`
	Jsz       Endpoint[scraper.JszOptions]          `json:"jsz"`
	Gatewayz  Endpoint[scraper.GatewayzOptions]     `json:"gatewayz"`
	Leafz     Endpoint[scraper.LeafzOptions]        `json:"leafz"`
	Accountz  Endpoint[scraper.AccountzOptions]     `json:"accountz"`
	Accstatz  Endpoint[scraper.AccountStatzOptions] `json:"accstatz"`
	Healthz   Endpoint[scraper.HealthzOptions]      `json:"healthz"`
	Raftz     Endpoint[scraper.RaftzOptions]        `json:"raftz"`
	Ipqueuesz Endpoint[scraper.IpqueueszOptions]    `json:"ipqueuesz"`
}

var DefaultConfig = Config{
	Logger:             slog.Default(),
	SubjectPrefix:      "scrape",
	StreamName:         "scrape",
	ScrapeTimeout:      30 * time.Second,
	ScrapeInterval:     20 * time.Second,
	SinkType:           "nats",
	SinkBatchSize:      500,
	OnDemandSubject:    "$SCR.ops.trigger",
	OnDemandQueueGroup: "",
	Varz:               Endpoint[scraper.VarzOptions]{Frequency: 1},
	Connz: Endpoint[scraper.ConnzOptions]{
		Frequency: 1,
		Options: scraper.ConnzOptions{
			Username:            true,
			Subscriptions:       true,
			SubscriptionsDetail: true,
			Limit:               1024,
		},
	},
	Subsz: Endpoint[scraper.SubszOptions]{
		Frequency: 1,
		Options: scraper.SubszOptions{
			Limit: 1024,
		},
	},
	Routez: Endpoint[scraper.RoutezOptions]{
		Frequency: 1,
		Options: scraper.RoutezOptions{
			SubscriptionsDetail: true,
		},
	},
	Jsz: Endpoint[scraper.JszOptions]{
		Frequency: 1,
		Options: scraper.JszOptions{
			Accounts:   true,
			Streams:    true,
			Consumer:   true,
			Config:     true,
			Limit:      10000,
			RaftGroups: true,
		},
	},
	Gatewayz: Endpoint[scraper.GatewayzOptions]{Frequency: 1},
	Leafz: Endpoint[scraper.LeafzOptions]{
		Frequency: 1,
		Options: scraper.LeafzOptions{
			Subscriptions: true,
		},
	},
	Accountz: Endpoint[scraper.AccountzOptions]{Frequency: 1},
	Accstatz: Endpoint[scraper.AccountStatzOptions]{
		Frequency: 1,
		Options: scraper.AccountStatzOptions{
			IncludeUnused: true,
		},
	},
	Healthz: Endpoint[scraper.HealthzOptions]{
		Frequency: 1,
		Options: scraper.HealthzOptions{
			Details: true,
		},
	},
	Raftz: Endpoint[scraper.RaftzOptions]{Frequency: 1},
	Ipqueuesz: Endpoint[scraper.IpqueueszOptions]{
		Frequency: 1,
		Options: scraper.IpqueueszOptions{
			All: true,
		},
	},
}
