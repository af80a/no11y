package scraper

import (
	"context"
	"log/slog"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const (
	ServerPingSubject   = "$SYS.REQ.SERVER.PING"
	VarzSubject         = "$SYS.REQ.SERVER.PING.VARZ"
	ConnzSubject        = "$SYS.REQ.SERVER.PING.CONNZ"
	SubszSubject        = "$SYS.REQ.SERVER.PING.SUBSZ"
	RoutezSubject       = "$SYS.REQ.SERVER.PING.ROUTEZ"
	JszSubject          = "$SYS.REQ.SERVER.PING.JSZ"
	GatewayzSubject     = "$SYS.REQ.SERVER.PING.GATEWAYZ"
	LeafzSubject        = "$SYS.REQ.SERVER.PING.LEAFZ"
	AccountzSubject     = "$SYS.REQ.SERVER.PING.ACCOUNTZ"
	AccountStatzSubject = "$SYS.REQ.ACCOUNT.PING.STATZ"
	HealthzSubject      = "$SYS.REQ.SERVER.PING.HEALTHZ"
	RaftzSubject        = "$SYS.REQ.SERVER.PING.RAFTZ"
	IpqueueszSubject    = "$SYS.REQ.SERVER.PING.IPQUEUESZ"
)

// Requestor is the collector-facing NATS monitoring request interface.
type Requestor interface {
	Request(context.Context, string, any) ([][]byte, error)
	RequestAdaptive(context.Context, string, any) ([][]byte, error)
	RequestOne(context.Context, string, any) ([]byte, error)
	ServerCount() int
	Timeout() time.Duration
}

// ServerInfo is the stable subset used for scrape targeting and filtering.
type ServerInfo struct {
	ID      string
	Name    string
	Cluster string
	Tags    []string
}

type VarzOptions struct{}

type AccountzOptions struct{}

func (o *AccountzOptions) toServerOptions() *server.AccountzEventOptions {
	return &server.AccountzEventOptions{}
}

type AccountStatzOptions struct {
	IncludeUnused bool `json:"include_unused,omitempty"`
}

func (o *AccountStatzOptions) toServerOptions() *server.AccountStatzEventOptions {
	if o == nil {
		o = &AccountStatzOptions{IncludeUnused: true}
	}
	return &server.AccountStatzEventOptions{
		AccountStatzOptions: server.AccountStatzOptions{IncludeUnused: o.IncludeUnused},
	}
}

type ConnzOptions struct {
	Username            bool `json:"auth,omitempty"`
	Subscriptions       bool `json:"subscriptions,omitempty"`
	SubscriptionsDetail bool `json:"subscriptions_detail,omitempty"`
	Limit               int  `json:"limit,omitempty"`
}

func (o *ConnzOptions) toServerOptions(offset int) *server.ConnzEventOptions {
	if o == nil {
		o = &ConnzOptions{
			Username:            true,
			Subscriptions:       true,
			SubscriptionsDetail: true,
			Limit:               1024,
		}
	}
	limit := o.Limit
	if limit == 0 {
		limit = 1024
	}
	return &server.ConnzEventOptions{
		ConnzOptions: server.ConnzOptions{
			Username:            o.Username,
			Subscriptions:       o.Subscriptions,
			SubscriptionsDetail: o.SubscriptionsDetail,
			Offset:              offset,
			Limit:               limit,
		},
	}
}

type GatewayzOptions struct {
	Accounts                   bool `json:"accounts,omitempty"`
	AccountSubscriptions       bool `json:"subscriptions,omitempty"`
	AccountSubscriptionsDetail bool `json:"subscriptions_detail,omitempty"`
}

func (o *GatewayzOptions) toServerOptions() *server.GatewayzEventOptions {
	if o == nil {
		o = &GatewayzOptions{}
	}
	return &server.GatewayzEventOptions{
		GatewayzOptions: server.GatewayzOptions{
			Accounts:                   o.Accounts,
			AccountSubscriptions:       o.AccountSubscriptions,
			AccountSubscriptionsDetail: o.AccountSubscriptionsDetail,
		},
	}
}

type HealthzOptions struct {
	Details bool `json:"details,omitempty"`
}

func (o *HealthzOptions) toServerOptions() *server.HealthzEventOptions {
	if o == nil {
		o = &HealthzOptions{Details: true}
	}
	return &server.HealthzEventOptions{
		HealthzOptions: server.HealthzOptions{Details: o.Details},
	}
}

type IpqueueszOptions struct {
	All    bool   `json:"all,omitempty"`
	Filter string `json:"filter,omitempty"`
}

func (o *IpqueueszOptions) toServerOptions() *server.IpqueueszEventOptions {
	if o == nil {
		o = &IpqueueszOptions{All: true}
	}
	return &server.IpqueueszEventOptions{
		IpqueueszOptions: server.IpqueueszOptions{All: o.All, Filter: o.Filter},
	}
}

type JszOptions struct {
	Accounts   bool `json:"accounts,omitempty"`
	Streams    bool `json:"streams,omitempty"`
	Consumer   bool `json:"consumer,omitempty"`
	Config     bool `json:"config,omitempty"`
	Limit      int  `json:"limit,omitempty"`
	RaftGroups bool `json:"raft,omitempty"`
}

func (o *JszOptions) toServerOptions(offset int) *server.JszEventOptions {
	if o == nil {
		o = &JszOptions{
			Accounts:   true,
			Streams:    true,
			Consumer:   true,
			Config:     true,
			Limit:      10000,
			RaftGroups: true,
		}
	}
	limit := o.Limit
	if limit == 0 {
		limit = 10000
	}
	return &server.JszEventOptions{
		JSzOptions: server.JSzOptions{
			Accounts:   o.Accounts,
			Streams:    o.Streams,
			Consumer:   o.Consumer,
			Config:     o.Config,
			Offset:     offset,
			Limit:      limit,
			RaftGroups: o.RaftGroups,
		},
	}
}

type LeafzOptions struct {
	Subscriptions bool   `json:"subscriptions,omitempty"`
	Account       string `json:"account,omitempty"`
}

func (o *LeafzOptions) toServerOptions() *server.LeafzEventOptions {
	if o == nil {
		o = &LeafzOptions{Subscriptions: true}
	}
	return &server.LeafzEventOptions{
		LeafzOptions: server.LeafzOptions{
			Subscriptions: o.Subscriptions,
			Account:       o.Account,
		},
	}
}

type RaftzOptions struct {
	Account string `json:"account,omitempty"`
	Group   string `json:"group,omitempty"`
}

func (o *RaftzOptions) toServerOptions() *server.RaftzEventOptions {
	if o == nil {
		o = &RaftzOptions{}
	}
	return &server.RaftzEventOptions{
		RaftzOptions: server.RaftzOptions{
			AccountFilter: o.Account,
			GroupFilter:   o.Group,
		},
	}
}

type RoutezOptions struct {
	Subscriptions       bool `json:"subscriptions,omitempty"`
	SubscriptionsDetail bool `json:"subscriptions_detail,omitempty"`
}

func (o *RoutezOptions) toServerOptions() *server.RoutezEventOptions {
	if o == nil {
		o = &RoutezOptions{SubscriptionsDetail: true}
	}
	return &server.RoutezEventOptions{
		RoutezOptions: server.RoutezOptions{
			Subscriptions:       o.Subscriptions,
			SubscriptionsDetail: o.SubscriptionsDetail,
		},
	}
}

type SubszOptions struct {
	Limit         int    `json:"limit,omitempty"`
	Subscriptions bool   `json:"subscriptions,omitempty"`
	Account       string `json:"account,omitempty"`
	Test          string `json:"test,omitempty"`
}

func (o *SubszOptions) toServerOptions(offset int) *server.SubszEventOptions {
	if o == nil {
		o = &SubszOptions{}
	}
	limit := o.Limit
	if limit <= 0 {
		limit = 1024
	}
	return &server.SubszEventOptions{
		SubszOptions: server.SubszOptions{
			Offset:        offset,
			Limit:         limit,
			Subscriptions: o.Subscriptions,
			Account:       o.Account,
			Test:          o.Test,
		},
	}
}

func NewRequestor(ids []string, nc *nats.Conn, timeout time.Duration, logger *slog.Logger, numExcluded int) Requestor {
	r := &requestor{
		ids:      ids,
		nc:       nc,
		timeout:  timeout,
		logger:   logger,
		targeted: numExcluded > 0,
	}
	if numExcluded > 0 {
		r.included = make(map[string]struct{}, len(ids))
		for _, id := range ids {
			r.included[id] = struct{}{}
		}
	}
	return r
}
