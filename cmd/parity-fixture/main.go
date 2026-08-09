package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"
)

type fixtureServer struct {
	connection      *nats.Conn
	pages           bool
	faults          bool
	raftGap         time.Duration
	raftDelay       time.Duration
	discovery       int
	discoveryError  bool
	accountzTimeout bool
	accountzFailure bool
	pingCount       atomic.Int64
	logRequests     bool
	started         time.Time
}

type requestObservation struct {
	Subject             string `json:"subject"`
	Offset              int64  `json:"offset,omitempty"`
	Account             string `json:"account,omitempty"`
	ElapsedMilliseconds int64  `json:"elapsed_ms"`
}

type fixtureInfo struct {
	id      string
	name    string
	cluster string
	tags    []string
}

var fixtureServers = []fixtureInfo{
	{id: "A", name: "alpha", cluster: "C1", tags: []string{"blue", "core"}},
	{id: "B", name: "beta", cluster: "C1", tags: []string{"green"}},
	{id: "C", name: "gamma", cluster: "C2", tags: []string{"edge"}},
}

func main() {
	host := flag.String("host", "127.0.0.1", "fixture listen host")
	port := flag.Int("port", 15800, "fixture listen port")
	pages := flag.Bool("pages", false, "force second CONNZ, JSZ, and SUBSZ pages")
	faults := flag.Bool("faults", false, "inject a VARZ error envelope and corrupt the second CONNZ page")
	raftGap := flag.Duration("raft-gap", 0, "stagger valid A/B/C RAFTZ replies by this interval")
	raftDelay := flag.Duration("raft-delay", 0, "delay the first valid RAFTZ reply by this duration")
	discovery := flag.Int("discovery-replies", -1, "limit only the first server PING to this many replies (-1=all)")
	discoveryError := flag.Bool("discovery-503", false, "answer only the first server PING with NATS status 503")
	accountzTimeout := flag.Bool("accountz-detail-timeout", false, "advertise FAIL only from A and leave its direct ACCOUNTZ detail unanswered")
	accountzFailure := flag.Bool("accountz-reporter-failure", false, "advertise six shared accounts and make reporter A reject every detail request")
	logRequests := flag.Bool("log-requests", false, "emit request subject, offset, and account as JSON")
	flag.Parse()

	if err := run(*host, *port, *pages, *faults, *raftGap, *raftDelay, *discovery, *discoveryError, *accountzTimeout, *accountzFailure, *logRequests); err != nil {
		fmt.Fprintln(os.Stderr, "parity fixture failed:", err)
		os.Exit(1)
	}
}

func run(host string, port int, pages, faults bool, raftGap, raftDelay time.Duration, discovery int, discoveryError, accountzTimeout, accountzFailure, logRequests bool) error {
	natsServer, err := server.NewServer(&server.Options{
		Host:            host,
		Port:            port,
		NoLog:           true,
		NoSigs:          true,
		NoSystemAccount: true,
	})
	if err != nil {
		return err
	}
	go natsServer.Start()
	if !natsServer.ReadyForConnections(5 * time.Second) {
		return errors.New("NATS fixture did not become ready")
	}
	defer natsServer.Shutdown()

	connection, err := nats.Connect(natsServer.ClientURL(), nats.Name("scraper-parity-fixture"))
	if err != nil {
		return err
	}
	defer connection.Close()
	fixture := &fixtureServer{
		connection:      connection,
		pages:           pages,
		faults:          faults,
		raftGap:         raftGap,
		raftDelay:       raftDelay,
		discovery:       discovery,
		discoveryError:  discoveryError,
		accountzTimeout: accountzTimeout,
		accountzFailure: accountzFailure,
		logRequests:     logRequests,
		started:         time.Now(),
	}
	subscription, err := connection.Subscribe("$SYS.REQ.>", fixture.respond)
	if err != nil {
		return err
	}
	defer subscription.Unsubscribe()
	if err := connection.Flush(); err != nil {
		return err
	}

	fmt.Printf(
		"fixture ready url=%s pages=%t faults=%t raft_gap=%s raft_delay=%s discovery_replies=%d discovery_503=%t accountz_detail_timeout=%t accountz_reporter_failure=%t\n",
		natsServer.ClientURL(),
		pages,
		faults,
		raftGap,
		raftDelay,
		discovery,
		discoveryError,
		accountzTimeout,
		accountzFailure,
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	return nil
}

func (f *fixtureServer) respond(request *nats.Msg) {
	options := gjson.ParseBytes(request.Data)
	if f.logRequests {
		_ = json.NewEncoder(os.Stdout).Encode(requestObservation{
			Subject:             request.Subject,
			Offset:              options.Get("offset").Int(),
			Account:             options.Get("account").String(),
			ElapsedMilliseconds: time.Since(f.started).Milliseconds(),
		})
	}

	switch {
	case request.Subject == "$SYS.REQ.SERVER.PING":
		firstPing := f.pingCount.Add(1) == 1
		if firstPing && f.discoveryError {
			f.replyStatus(request, "503", "No Responders")
			return
		}
		servers := fixtureServers
		if firstPing && f.discovery >= 0 && f.discovery < len(servers) {
			servers = servers[:f.discovery]
		}
		for _, info := range servers {
			f.reply(request, pingEnvelope(info.id, info.name, info.cluster, info.tags))
		}
	case request.Subject == "$SYS.REQ.ACCOUNT.PING.STATZ":
		for _, info := range fixtureServers {
			f.reply(request, endpointEnvelope(info.id, info.name, `{"accounts":[]}`))
		}
	case strings.HasPrefix(request.Subject, "$SYS.REQ.SERVER.PING."):
		endpoint := strings.TrimPrefix(request.Subject, "$SYS.REQ.SERVER.PING.")
		if endpoint == "RAFTZ" && (f.raftGap > 0 || f.raftDelay > 0) {
			f.replyRaftzStaggered(request.Reply, options.Get("account").String())
			return
		}
		for _, info := range fixtureServers {
			f.replyEndpoint(request, info.id, info.name, endpoint, options)
		}
	case strings.HasPrefix(request.Subject, "$SYS.REQ.SERVER."):
		tokens := strings.Split(request.Subject, ".")
		if len(tokens) != 5 {
			return
		}
		if info, ok := serverByID(tokens[3]); ok {
			f.replyEndpoint(request, info.id, info.name, tokens[4], options)
		}
	}
}

func (f *fixtureServer) replyEndpoint(request *nats.Msg, id, name, endpoint string, options gjson.Result) {
	if f.faults && endpoint == "VARZ" && id == "B" {
		f.reply(request, []byte(`{"server":{"id":"B","name":"beta"},"error":{"code":500,"description":"fixture error"}}`))
		return
	}
	if f.faults && endpoint == "CONNZ" && id == "A" && options.Get("offset").Int() == 1025 {
		message := nats.NewMsg(request.Reply)
		message.Header.Set("Content-Encoding", "snappy")
		message.Data = []byte("invalid-s2-frame")
		_ = f.connection.PublishMsg(message)
		return
	}
	switch endpoint {
	case "ACCOUNTZ":
		account := options.Get("account").String()
		if account == "" {
			accounts := `{"accounts":["$G","APP1","SYS"]}`
			if f.accountzFailure {
				accounts = `{"accounts":["A1","A2","A3","A4","A5","A6"]}`
			}
			if f.accountzTimeout && id == "A" {
				accounts = `{"accounts":["$G","APP1","SYS","FAIL"]}`
			}
			f.reply(request, endpointEnvelope(id, name, accounts))
			return
		}
		if f.accountzTimeout && id == "A" && account == "FAIL" {
			return
		}
		if f.accountzFailure && id == "A" {
			f.replyStatus(request, "503", "Reporter Unavailable")
			return
		}
		f.reply(request, endpointEnvelope(id, name, fmt.Sprintf(`{"account_detail":{"account_name":%q}}`, account)))
	case "RAFTZ":
		if id == "C" && f.raftGap == 0 {
			f.reply(request, endpointEnvelope(id, name, `{}`))
			return
		}
		account := options.Get("account").String()
		f.reply(request, endpointEnvelope(id, name, fmt.Sprintf(`{"groups":{"%s-%s":{"name":%q}}}`, account, id, account+"-"+id)))
	case "CONNZ", "SUBSZ":
		total := int64(1)
		if f.pages {
			total = 1026
		}
		f.reply(request, endpointEnvelope(id, name, fmt.Sprintf(`{"total":%d,"offset":%d,"limit":1024}`, total, options.Get("offset").Int())))
	case "JSZ":
		accounts := int64(1)
		if f.pages {
			accounts = 10001
		}
		f.reply(request, endpointEnvelope(id, name, fmt.Sprintf(`{"accounts":%d,"total":%d,"offset":%d,"limit":10000}`, accounts, accounts, options.Get("offset").Int())))
	default:
		f.reply(request, endpointEnvelope(id, name, `{}`))
	}
}

func (f *fixtureServer) replyRaftzStaggered(reply, account string) {
	for index, info := range fixtureServers {
		info := info
		delay := f.raftDelay + time.Duration(index)*f.raftGap
		time.AfterFunc(delay, func() {
			payload := endpointEnvelope(
				info.id,
				info.name,
				fmt.Sprintf(`{"groups":{"%s-%s":{"name":%q}}}`, account, info.id, account+"-"+info.id),
			)
			_ = f.connection.Publish(reply, payload)
		})
	}
}

func (f *fixtureServer) reply(request *nats.Msg, payload []byte) {
	_ = request.Respond(payload)
}

func (f *fixtureServer) replyStatus(request *nats.Msg, status, description string) {
	message := nats.NewMsg(request.Reply)
	message.Header.Set("Status", status)
	message.Header.Set("Description", description)
	_ = f.connection.PublishMsg(message)
}

func pingEnvelope(id, name, cluster string, tags []string) []byte {
	tagsJSON, _ := json.Marshal(tags)
	return []byte(fmt.Sprintf(
		`{"server":{"id":%q,"name":%q,"cluster":%q,"tags":%s},"statsz":{}}`,
		id,
		name,
		cluster,
		tagsJSON,
	))
}

func endpointEnvelope(id, name, data string) []byte {
	return []byte(fmt.Sprintf(`{"server":{"id":%q,"name":%q},"data":%s}`, id, name, data))
}

func serverByID(id string) (fixtureInfo, bool) {
	for _, info := range fixtureServers {
		if info.id == id {
			return info, true
		}
	}
	return fixtureInfo{}, false
}
