package scraper

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type fakeRequestor struct {
	request      func(context.Context, string, any) ([][]byte, error)
	one          func(context.Context, string, any) ([]byte, error)
	count        int
	timeout      time.Duration
	timeoutCalls atomic.Int32
}

func TestCurrentActiveServerInfosTranslatesStatus503(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	subscription, err := nc.Subscribe(ServerPingSubject, func(request *nats.Msg) {
		response := nats.NewMsg(request.Reply)
		response.Header.Set("Status", "503")
		response.Header.Set("Description", "No Responders")
		_ = nc.PublishMsg(response)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	infos, err := CurrentActiveServerInfos(context.Background(), logger, nc, time.Second)
	if err == nil || err.Error() != systemRequestError {
		t.Fatalf("discovery error = %v, want %q", err, systemRequestError)
	}
	if len(infos) != 0 {
		t.Fatalf("discovery infos = %#v, want none", infos)
	}
}

func (f *fakeRequestor) Request(ctx context.Context, subject string, options any) ([][]byte, error) {
	return f.request(ctx, subject, options)
}

func (f *fakeRequestor) RequestAdaptive(ctx context.Context, subject string, options any) ([][]byte, error) {
	return f.request(ctx, subject, options)
}

func (f *fakeRequestor) RequestOne(ctx context.Context, subject string, options any) ([]byte, error) {
	return f.one(ctx, subject, options)
}

func (f *fakeRequestor) ServerCount() int { return f.count }
func (f *fakeRequestor) Timeout() time.Duration {
	f.timeoutCalls.Add(1)
	if f.timeout == 0 {
		return time.Second
	}
	return f.timeout
}

func TestServersPreservesPartialResponsesWithError(t *testing.T) {
	want := [][]byte{[]byte(`{"server":{"id":"A"}}`)}
	wantErr := errors.New("request failed after one response")
	fake := &fakeRequestor{request: func(context.Context, string, any) ([][]byte, error) {
		return want, wantErr
	}}

	got, err := Servers(context.Background(), fake)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Servers error = %v, want %v", err, wantErr)
	}
	if len(got) != 1 || string(got[0]) != string(want[0]) {
		t.Fatalf("Servers responses = %q, want %q", got, want)
	}
}

func TestCurrentActiveServerInfosPreservesOrderAndSkipsMalformedDuplicates(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	sub, err := nc.Subscribe(ServerPingSubject, func(request *nats.Msg) {
		for _, response := range [][]byte{
			[]byte(`{"server":{"id":"B","name":"second","cluster":"C","tags":["blue"]}}`),
			[]byte(`not-json`),
			[]byte(`{"server":{"id":"B","name":"duplicate"}}`),
			[]byte(`{"server":{"id":"A","name":"first"}}`),
		} {
			_ = request.Respond(response)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	infos, err := CurrentActiveServerInfos(ctx, slog.Default(), nc, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 {
		t.Fatalf("server infos = %#v, want two unique valid replies", infos)
	}
	if infos[0].ID != "B" || infos[0].Name != "second" || infos[0].Cluster != "C" || len(infos[0].Tags) != 1 || infos[0].Tags[0] != "blue" {
		t.Fatalf("first server = %#v", infos[0])
	}
	if infos[1].ID != "A" || infos[1].Name != "first" {
		t.Fatalf("second server = %#v", infos[1])
	}
}

func TestConnzPaginatesRawResponseEnvelopes(t *testing.T) {
	var offsets []int
	fake := &fakeRequestor{count: 1}
	fake.request = func(_ context.Context, subject string, options any) ([][]byte, error) {
		if subject != ConnzSubject {
			t.Fatalf("subject = %q", subject)
		}
		offset := options.(*server.ConnzEventOptions).Offset
		offsets = append(offsets, offset)
		if offset == 0 {
			return [][]byte{[]byte(`{"server":{"id":"S1"},"data":{"total":3,"offset":0,"limit":2}}`)}, nil
		}
		return [][]byte{[]byte(`{"server":{"id":"S1"},"data":{"total":3,"offset":3,"limit":2}}`)}, nil
	}

	responses, err := Connz(context.Background(), slog.Default(), fake, &ConnzOptions{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want 2", len(responses))
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 3 {
		t.Fatalf("offsets = %v, want [0 3]", offsets)
	}
}

func TestJszPaginatesWithoutInclusiveOffset(t *testing.T) {
	var offsets []int
	fake := &fakeRequestor{count: 1}
	fake.request = func(_ context.Context, _ string, options any) ([][]byte, error) {
		offset := options.(*server.JszEventOptions).Offset
		offsets = append(offsets, offset)
		if offset == 0 {
			return [][]byte{[]byte(`{"server":{"id":"S1"},"data":{"accounts":3}}`)}, nil
		}
		return [][]byte{[]byte(`{"server":{"id":"S1"},"data":{"accounts":3}}`)}, nil
	}

	if _, err := Jsz(context.Background(), slog.Default(), fake, &JszOptions{Limit: 2}); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 2 {
		t.Fatalf("offsets = %v, want [0 2]", offsets)
	}
}

func TestJszIgnoresTotalWhenDecidingWhetherToPaginate(t *testing.T) {
	calls := 0
	fake := &fakeRequestor{count: 1}
	fake.request = func(_ context.Context, _ string, _ any) ([][]byte, error) {
		calls++
		return [][]byte{[]byte(`{"server":{"id":"S1"},"data":{"total":10001}}`)}, nil
	}

	if _, err := Jsz(context.Background(), slog.Default(), fake, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("request calls = %d, want 1", calls)
	}
}

func TestPagedCollectorDiscardsEarlierPagesOnTransportError(t *testing.T) {
	wantErr := errors.New("second page failed")
	calls := 0
	fake := &fakeRequestor{count: 1}
	fake.request = func(_ context.Context, _ string, _ any) ([][]byte, error) {
		calls++
		if calls == 1 {
			return [][]byte{[]byte(`{"server":{"id":"S1"},"data":{"accounts":3}}`)}, nil
		}
		return nil, wantErr
	}

	responses, err := Jsz(context.Background(), slog.Default(), fake, &JszOptions{Limit: 2})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Jsz error = %v, want %v", err, wantErr)
	}
	if responses != nil {
		t.Fatalf("Jsz responses = %q, want nil after later transport error", responses)
	}
}

func TestCollectorOmitsMonitoringErrorResponses(t *testing.T) {
	fake := &fakeRequestor{count: 2}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{
			[]byte(`{"server":{"id":"A"},"error":{"code":503,"description":"unavailable"}}`),
			[]byte(`{"server":{"id":"B"},"data":{}}`),
		}, nil
	}

	responses, err := Varz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || !strings.Contains(string(responses[0]), `"id":"B"`) {
		t.Fatalf("responses = %s, want only server B", responses)
	}
}

func TestCollectorAllMonitoringErrorsReturnsNil(t *testing.T) {
	fake := &fakeRequestor{count: 1}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{
			[]byte(`{"server":{"id":"A"},"error":{"code":503,"description":"unavailable"}}`),
		}, nil
	}

	responses, err := Varz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if responses != nil {
		t.Fatalf("responses = %q, want nil", responses)
	}
}

func TestConnzPaginationUsesLastValidResponse(t *testing.T) {
	errorResponse := []byte(`{"server":{"name":"S"},"error":{"description":"unavailable"}}`)
	tests := []struct {
		name      string
		firstPage [][]byte
		wantCalls int
	}{
		{
			name: "last valid response stops",
			firstPage: [][]byte{
				[]byte(`{"data":{"total":10}}`),
				[]byte(`{"data":{"total":0}}`),
			},
			wantCalls: 1,
		},
		{
			name: "last valid response continues",
			firstPage: [][]byte{
				[]byte(`{"data":{"total":0}}`),
				[]byte(`{"data":{"total":10}}`),
			},
			wantCalls: 2,
		},
		{
			name: "error preserves prior decision",
			firstPage: [][]byte{
				[]byte(`{"data":{"total":10}}`),
				errorResponse,
			},
			wantCalls: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var offsets []int
			fake := &fakeRequestor{count: 2}
			fake.request = func(_ context.Context, subject string, options any) ([][]byte, error) {
				if subject != ConnzSubject {
					t.Fatalf("subject = %q", subject)
				}
				offset := options.(*server.ConnzEventOptions).Offset
				offsets = append(offsets, offset)
				if len(offsets) == 1 {
					return test.firstPage, nil
				}
				return [][]byte{[]byte(`{"data":{"total":0}}`)}, nil
			}

			if _, err := Connz(context.Background(), slog.Default(), fake, &ConnzOptions{Limit: 2}); err != nil {
				t.Fatal(err)
			}
			if len(offsets) != test.wantCalls {
				t.Fatalf("request calls = %d, want %d", len(offsets), test.wantCalls)
			}
			if test.wantCalls == 2 && offsets[1] != 3 {
				t.Fatalf("second offset = %d, want 3", offsets[1])
			}
		})
	}
}

func TestRecoveredOptionDefaults(t *testing.T) {
	connz := (*ConnzOptions)(nil).toServerOptions(0)
	if !connz.Username || !connz.Subscriptions || !connz.SubscriptionsDetail || connz.Limit != 1024 {
		t.Fatalf("connz defaults = %#v", connz.ConnzOptions)
	}
	jsz := (*JszOptions)(nil).toServerOptions(0)
	if !jsz.Accounts || !jsz.Streams || !jsz.Consumer || !jsz.Config || !jsz.RaftGroups || jsz.Limit != 10000 {
		t.Fatalf("jsz defaults = %#v", jsz.JSzOptions)
	}
	subsz := (*SubszOptions)(nil).toServerOptions(0)
	if subsz.Subscriptions || subsz.Limit != 1024 {
		t.Fatalf("subsz defaults = %#v", subsz.SubszOptions)
	}
	routez := (*RoutezOptions)(nil).toServerOptions()
	if routez.Subscriptions || !routez.SubscriptionsDetail {
		t.Fatalf("routez defaults = %#v", routez.RoutezOptions)
	}
	if !(*AccountStatzOptions)(nil).toServerOptions().IncludeUnused {
		t.Fatal("account statz should include unused accounts by default")
	}
	if !(*HealthzOptions)(nil).toServerOptions().Details {
		t.Fatal("healthz should include details by default")
	}
	if !(*IpqueueszOptions)(nil).toServerOptions().All {
		t.Fatal("ipqueuesz should include all queues by default")
	}
	if !(*LeafzOptions)(nil).toServerOptions().Subscriptions {
		t.Fatal("leafz should include subscriptions by default")
	}
	if got := (*AccountzOptions)(nil).toServerOptions(); got == nil {
		t.Fatal("accountz options should produce a server request object")
	}
}

func TestRecoveredOptionLayouts(t *testing.T) {
	info := ServerInfo{}
	if unsafe.Sizeof(info) != 72 || unsafe.Offsetof(info.ID) != 0 || unsafe.Offsetof(info.Name) != 16 || unsafe.Offsetof(info.Cluster) != 32 || unsafe.Offsetof(info.Tags) != 48 {
		t.Fatalf("ServerInfo layout changed: size=%d", unsafe.Sizeof(info))
	}
	connz := ConnzOptions{}
	if unsafe.Sizeof(connz) != 16 || unsafe.Offsetof(connz.Username) != 0 || unsafe.Offsetof(connz.Subscriptions) != 1 || unsafe.Offsetof(connz.SubscriptionsDetail) != 2 || unsafe.Offsetof(connz.Limit) != 8 {
		t.Fatalf("ConnzOptions layout changed: size=%d", unsafe.Sizeof(connz))
	}
	ipqueuesz := IpqueueszOptions{}
	if unsafe.Sizeof(ipqueuesz) != 24 || unsafe.Offsetof(ipqueuesz.All) != 0 || unsafe.Offsetof(ipqueuesz.Filter) != 8 {
		t.Fatalf("IpqueueszOptions layout changed: size=%d", unsafe.Sizeof(ipqueuesz))
	}
	jsz := JszOptions{}
	if unsafe.Sizeof(jsz) != 24 || unsafe.Offsetof(jsz.Accounts) != 0 || unsafe.Offsetof(jsz.Streams) != 1 || unsafe.Offsetof(jsz.Consumer) != 2 || unsafe.Offsetof(jsz.Config) != 3 || unsafe.Offsetof(jsz.Limit) != 8 || unsafe.Offsetof(jsz.RaftGroups) != 16 {
		t.Fatalf("JszOptions layout changed: size=%d", unsafe.Sizeof(jsz))
	}
	leafz := LeafzOptions{}
	if unsafe.Sizeof(leafz) != 24 || unsafe.Offsetof(leafz.Subscriptions) != 0 || unsafe.Offsetof(leafz.Account) != 8 {
		t.Fatalf("LeafzOptions layout changed: size=%d", unsafe.Sizeof(leafz))
	}
	raftz := RaftzOptions{}
	if unsafe.Sizeof(raftz) != 32 || unsafe.Offsetof(raftz.Account) != 0 || unsafe.Offsetof(raftz.Group) != 16 {
		t.Fatalf("RaftzOptions layout changed: size=%d", unsafe.Sizeof(raftz))
	}
	subsz := SubszOptions{}
	if unsafe.Sizeof(subsz) != 48 || unsafe.Offsetof(subsz.Limit) != 0 || unsafe.Offsetof(subsz.Subscriptions) != 8 || unsafe.Offsetof(subsz.Account) != 16 || unsafe.Offsetof(subsz.Test) != 32 {
		t.Fatalf("SubszOptions layout changed: size=%d", unsafe.Sizeof(subsz))
	}
}

func TestRecoveredOptionJSONTags(t *testing.T) {
	tests := []struct {
		value any
		tags  []string
	}{
		{AccountStatzOptions{}, []string{"include_unused,omitempty"}},
		{ConnzOptions{}, []string{"auth,omitempty", "subscriptions,omitempty", "subscriptions_detail,omitempty", "limit,omitempty"}},
		{GatewayzOptions{}, []string{"accounts,omitempty", "subscriptions,omitempty", "subscriptions_detail,omitempty"}},
		{HealthzOptions{}, []string{"details,omitempty"}},
		{IpqueueszOptions{}, []string{"all,omitempty", "filter,omitempty"}},
		{JszOptions{}, []string{"accounts,omitempty", "streams,omitempty", "consumer,omitempty", "config,omitempty", "limit,omitempty", "raft,omitempty"}},
		{LeafzOptions{}, []string{"subscriptions,omitempty", "account,omitempty"}},
		{RaftzOptions{}, []string{"account,omitempty", "group,omitempty"}},
		{RoutezOptions{}, []string{"subscriptions,omitempty", "subscriptions_detail,omitempty"}},
		{SubszOptions{}, []string{"limit,omitempty", "subscriptions,omitempty", "account,omitempty", "test,omitempty"}},
	}
	for _, test := range tests {
		typeOf := reflect.TypeOf(test.value)
		for i, want := range test.tags {
			if got := typeOf.Field(i).Tag.Get("json"); got != want {
				t.Errorf("%s.%s JSON tag = %q, want %q", typeOf.Name(), typeOf.Field(i).Name, got, want)
			}
		}
		encoded, err := json.Marshal(test.value)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != "{}" {
			t.Errorf("zero %s JSON = %s, want {}", typeOf.Name(), encoded)
		}
	}
}

func TestAccountzDiscoversThenFetchesAccountDetails(t *testing.T) {
	fake := &fakeRequestor{count: 2}
	fake.request = func(_ context.Context, subject string, _ any) ([][]byte, error) {
		if subject != AccountzSubject {
			t.Fatalf("subject = %q", subject)
		}
		return [][]byte{
			[]byte(`{"server":{"id":"A"},"data":{"accounts":["ONE","SHARED"]}}`),
			[]byte(`{"server":{"id":"B"},"data":{"accounts":["TWO","SHARED"]}}`),
		}, nil
	}
	fake.one = func(_ context.Context, subject string, options any) ([]byte, error) {
		account := options.(*server.AccountzEventOptions).Account
		if !strings.HasSuffix(subject, ".ACCOUNTZ") {
			t.Fatalf("detail subject = %q", subject)
		}
		return []byte(`{"server":{"id":"selected"},"data":{"account_detail":{"account_name":"` + account + `"}}}`), nil
	}

	responses, err := Accountz(context.Background(), slog.Default(), fake, &AccountzOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 3 {
		t.Fatalf("responses = %d, want one per unique account", len(responses))
	}
	if got := fake.timeoutCalls.Load(); got != 1 {
		t.Fatalf("Timeout calls = %d, want 1", got)
	}
}

func TestAccountzKeepsUsingFirstHealthyReporter(t *testing.T) {
	fake := &fakeRequestor{count: 1}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{
			[]byte(`{"server":{"id":"A"},"data":{"accounts":["ONE","TWO","THREE"]}}`),
			[]byte(`{"server":{"id":"B"},"data":{"accounts":["ONE","TWO","THREE"]}}`),
		}, nil
	}
	var subjects []string
	fake.one = func(_ context.Context, subject string, options any) ([]byte, error) {
		subjects = append(subjects, subject)
		account := options.(*server.AccountzEventOptions).Account
		return []byte(`{"server":{"id":"A"},"data":{"account_detail":{"account_name":"` + account + `"}}}`), nil
	}

	responses, err := Accountz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 3 {
		t.Fatalf("responses = %d, want 3", len(responses))
	}
	for _, subject := range subjects {
		if subject != "$SYS.REQ.SERVER.A.ACCOUNTZ" {
			t.Fatalf("detail subjects = %v, want first reporter A for every healthy request", subjects)
		}
	}
}

func TestAccountzAvoidsFailedReportersThenFallsBackRandomly(t *testing.T) {
	fake := &fakeRequestor{count: 1}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{
			[]byte(`{"server":{"id":"A"},"data":{"accounts":["ONE","TWO","THREE"]}}`),
			[]byte(`{"server":{"id":"B"},"data":{"accounts":["ONE","TWO","THREE"]}}`),
		}, nil
	}
	var subjects []string
	fake.one = func(_ context.Context, subject string, options any) ([]byte, error) {
		subjects = append(subjects, subject)
		if len(subjects) <= 2 {
			return nil, errors.New("reporter unavailable")
		}
		account := options.(*server.AccountzEventOptions).Account
		return []byte(`{"server":{"id":"fallback"},"data":{"account_detail":{"account_name":"` + account + `"}}}`), nil
	}

	responses, err := Accountz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(subjects) != 3 {
		t.Fatalf("detail subjects = %v, want three attempts", subjects)
	}
	if subjects[0] != "$SYS.REQ.SERVER.A.ACCOUNTZ" || subjects[1] != "$SYS.REQ.SERVER.B.ACCOUNTZ" {
		t.Fatalf("detail subjects = %v, want first-unused reporter order A then B", subjects)
	}
	if subjects[2] != "$SYS.REQ.SERVER.A.ACCOUNTZ" && subjects[2] != "$SYS.REQ.SERVER.B.ACCOUNTZ" {
		t.Fatalf("fallback subject = %q, want a random known reporter", subjects[2])
	}
	if len(responses) != 1 {
		t.Fatalf("responses = %d, want only the successful fallback", len(responses))
	}
}

func TestAccountzDoesNotMarkReporterFailedForInvalidPayload(t *testing.T) {
	fake := &fakeRequestor{count: 1}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{
			[]byte(`{"server":{"id":"A"},"data":{"accounts":["ONE","TWO"]}}`),
			[]byte(`{"server":{"id":"B"},"data":{"accounts":["ONE","TWO"]}}`),
		}, nil
	}
	var subjects []string
	fake.one = func(_ context.Context, subject string, options any) ([]byte, error) {
		subjects = append(subjects, subject)
		if len(subjects) == 1 {
			return []byte(`not-json`), nil
		}
		account := options.(*server.AccountzEventOptions).Account
		return []byte(`{"server":{"id":"A"},"data":{"account_detail":{"account_name":"` + account + `"}}}`), nil
	}

	responses, err := Accountz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(subjects) != 2 || subjects[0] != "$SYS.REQ.SERVER.A.ACCOUNTZ" || subjects[1] != "$SYS.REQ.SERVER.A.ACCOUNTZ" {
		t.Fatalf("detail subjects = %v, want invalid payload to leave reporter A eligible", subjects)
	}
	if len(responses) != 1 {
		t.Fatalf("responses = %d, want only the valid payload", len(responses))
	}
}

func TestRaftzUsesAccountzInventoryAndIgnoresConfiguredAccount(t *testing.T) {
	fake := &fakeRequestor{count: 1}
	requested := make(map[string]bool)
	fake.request = func(_ context.Context, subject string, options any) ([][]byte, error) {
		if subject == AccountzSubject {
			return [][]byte{[]byte(`{"server":{"id":"A"},"data":{"accounts":["ONE","TWO"]}}`)}, nil
		}
		request := options.(*server.RaftzEventOptions)
		if subject != RaftzSubject {
			t.Fatalf("subject = %q", subject)
		}
		if request.GroupFilter != "G" {
			t.Fatalf("options = %#v", request.RaftzOptions)
		}
		requested[request.AccountFilter] = true
		return [][]byte{[]byte(`{"server":{"id":"A"},"data":{"groups":1}}`)}, nil
	}

	responses, err := Raftz(context.Background(), slog.Default(), fake, &RaftzOptions{Account: "TWO", Group: "G"})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want one for every discovered account", len(responses))
	}
	if !requested["ONE"] || !requested["TWO"] {
		t.Fatalf("requested accounts = %v, want ONE and TWO", requested)
	}
	if got := fake.timeoutCalls.Load(); got != 1 {
		t.Fatalf("Timeout calls = %d, want 1", got)
	}
}

func TestRaftzOmitsEmptyDataAndMonitoringErrors(t *testing.T) {
	fake := &fakeRequestor{count: 1}
	fake.request = func(_ context.Context, subject string, options any) ([][]byte, error) {
		if subject == AccountzSubject {
			return [][]byte{[]byte(`{"data":{"accounts":["EMPTY","ERROR","VALID"]}}`)}, nil
		}
		switch options.(*server.RaftzEventOptions).AccountFilter {
		case "EMPTY":
			return [][]byte{[]byte(`{"data":{}}`)}, nil
		case "ERROR":
			return [][]byte{[]byte(`{"server":{"name":"S"},"error":{"description":"unavailable"}}`)}, nil
		default:
			return [][]byte{[]byte(`{"data":{"group":{"name":"G"}}`)}, nil
		}
	}

	responses, err := Raftz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || !strings.Contains(string(responses[0]), `"name":"G"`) {
		t.Fatalf("responses = %q, want only non-empty data", responses)
	}
}

func TestRaftzAdaptiveHonorsTargetedServerFilter(t *testing.T) {
	_, nc := runTestNATSServer(t, false)
	inventory, err := nc.Subscribe("$SYS.REQ.SERVER.INCLUDED.ACCOUNTZ", func(request *nats.Msg) {
		_ = request.Respond([]byte(`{"server":{"id":"INCLUDED"},"data":{"accounts":["A"]}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer inventory.Unsubscribe()
	raft, err := nc.Subscribe("$SYS.REQ.SERVER.INCLUDED.RAFTZ", func(request *nats.Msg) {
		_ = request.Respond([]byte(`{"server":{"id":"INCLUDED"},"data":{"groups":1}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer raft.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	requestor := NewRequestor(
		[]string{"INCLUDED"},
		nc,
		time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		1,
	)
	responses, err := Raftz(context.Background(), slog.Default(), requestor, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || !strings.Contains(string(responses[0]), `"id":"INCLUDED"`) {
		t.Fatalf("responses = %q, want included targeted payload", responses)
	}
}

func TestRaftzRetainsMultipleAdaptiveRepliesPerAccount(t *testing.T) {
	fake := &fakeRequestor{count: 1}
	fake.request = func(_ context.Context, subject string, _ any) ([][]byte, error) {
		if subject == AccountzSubject {
			return [][]byte{[]byte(`{"data":{"accounts":["A"]}}`)}, nil
		}
		return [][]byte{
			[]byte(`{"server":{"id":"S1"},"data":{"groups":1}}`),
			[]byte(`{"server":{"id":"S2"},"data":{"groups":1}}`),
		}, nil
	}

	responses, err := Raftz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %d, want both adaptive replies", len(responses))
	}
}

func TestAccountScopedDetailFailuresAreOmittedWithoutFallback(t *testing.T) {
	fake := &fakeRequestor{count: 2}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{
			[]byte(`{"server":{"id":"A"},"data":{"accounts":["ONE"]}}`),
			[]byte(`{"server":{"id":"B"},"data":{"accounts":["ONE"]}}`),
		}, nil
	}
	var attempts atomic.Int32
	fake.one = func(context.Context, string, any) ([]byte, error) {
		attempts.Add(1)
		return nil, errors.New("detail unavailable")
	}

	responses, err := Accountz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatalf("Accountz returned detail error: %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("responses = %d, want failed detail omitted", len(responses))
	}
	if attempts.Load() != 1 {
		t.Fatalf("detail attempts = %d, want one selected reporter with no same-account fallback", attempts.Load())
	}
}

func TestAccountzOmitsInvalidDetailEnvelopes(t *testing.T) {
	fake := &fakeRequestor{count: 2}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{[]byte(`{"server":{"id":"A"},"data":{"accounts":["BAD","ERR","OK"]}}`)}, nil
	}
	fake.one = func(_ context.Context, _ string, options any) ([]byte, error) {
		switch options.(*server.AccountzEventOptions).Account {
		case "BAD":
			return []byte(`not-json`), nil
		case "ERR":
			return []byte(`{"error":{"code":503,"description":"unavailable"}}`), nil
		default:
			return []byte(`{"server":{"id":"A"},"data":{"account_detail":{"account_name":"OK"}}}`), nil
		}
	}

	responses, err := Accountz(context.Background(), slog.Default(), fake, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || !strings.Contains(string(responses[0]), `"account_name":"OK"`) {
		t.Fatalf("responses = %q, want only valid detail", responses)
	}
}

func TestAccountzUsesServerCountWorkerPool(t *testing.T) {
	const accountCount = 6
	fake := &fakeRequestor{count: 2}
	fake.request = func(context.Context, string, any) ([][]byte, error) {
		return [][]byte{[]byte(`{"server":{"id":"A"},"data":{"accounts":["A1","A2","A3","A4","A5","A6"]}}`)}, nil
	}
	var active, maximum, started atomic.Int32
	release := make(chan struct{})
	fake.one = func(context.Context, string, any) ([]byte, error) {
		current := active.Add(1)
		started.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		<-release
		active.Add(-1)
		return []byte(`{"server":{"id":"A"},"data":{}}`), nil
	}

	done := make(chan error, 1)
	go func() {
		responses, err := Accountz(context.Background(), slog.Default(), fake, nil)
		if err == nil && len(responses) != accountCount {
			err = errors.New("not all account details were retained")
		}
		done <- err
	}()

	deadline := time.After(time.Second)
	for started.Load() < int32(fake.count) {
		select {
		case <-deadline:
			t.Fatal("worker pool did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if got := started.Load(); got != int32(fake.count) {
		t.Fatalf("started requests = %d before release, want %d", got, fake.count)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got != int32(fake.count) {
		t.Fatalf("maximum concurrency = %d, want %d", got, fake.count)
	}
}

func TestAccountDetailTimeoutClamp(t *testing.T) {
	tests := []struct {
		timeout time.Duration
		want    time.Duration
	}{
		{timeout: 600 * time.Millisecond, want: time.Second},
		{timeout: 9 * time.Second, want: 3 * time.Second},
		{timeout: 30 * time.Second, want: 5 * time.Second},
	}
	for _, test := range tests {
		if got := clampEntityTimeout(test.timeout); got != test.want {
			t.Errorf("clampEntityTimeout(%v) = %v, want %v", test.timeout, got, test.want)
		}
	}
}

func TestEndpointOptionJSONMatchesServerContract(t *testing.T) {
	encoded, err := json.Marshal((&GatewayzOptions{
		Accounts:                   true,
		AccountSubscriptions:       true,
		AccountSubscriptionsDetail: true,
	}).toServerOptions())
	if err != nil {
		t.Fatal(err)
	}
	got := string(encoded)
	for _, field := range []string{`"accounts":true`, `"subscriptions":true`, `"subscriptions_detail":true`} {
		if !strings.Contains(got, field) {
			t.Fatalf("encoded options %s missing %s", got, field)
		}
	}
}

func TestRuntimeObservedDefaultRequestJSON(t *testing.T) {
	tests := []struct {
		name    string
		options any
		want    string
	}{
		{name: "varz", options: &server.VarzEventOptions{}, want: `{}`},
		{name: "accountz", options: (*AccountzOptions)(nil).toServerOptions(), want: `{"account":""}`},
		{name: "connz", options: (*ConnzOptions)(nil).toServerOptions(0), want: `{"sort":"","auth":true,"subscriptions":true,"subscriptions_detail":true,"offset":0,"limit":1024,"cid":0,"mqtt_client":"","state":0,"user":"","acc":"","filter_subject":""}`},
		{name: "jsz", options: (*JszOptions)(nil).toServerOptions(0), want: `{"accounts":true,"streams":true,"consumer":true,"config":true,"limit":10000,"raft":true}`},
		{name: "routez", options: (*RoutezOptions)(nil).toServerOptions(), want: `{"subscriptions":false,"subscriptions_detail":true}`},
		{name: "subsz", options: (*SubszOptions)(nil).toServerOptions(0), want: `{"offset":0,"limit":1024,"subscriptions":false}`},
		{name: "gatewayz", options: (*GatewayzOptions)(nil).toServerOptions(), want: `{"name":"","accounts":false,"account_name":"","subscriptions":false,"subscriptions_detail":false}`},
		{name: "leafz", options: (*LeafzOptions)(nil).toServerOptions(), want: `{"subscriptions":true,"account":""}`},
		{name: "accstatz", options: (*AccountStatzOptions)(nil).toServerOptions(), want: `{"accounts":null,"include_unused":true}`},
		{name: "healthz", options: (*HealthzOptions)(nil).toServerOptions(), want: `{"details":true}`},
		{name: "raftz", options: (&RaftzOptions{Account: "SYS"}).toServerOptions(), want: `{"account":"SYS","group":""}`},
		{name: "ipqueuesz", options: (*IpqueueszOptions)(nil).toServerOptions(), want: `{"all":true,"filter":""}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.options)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(encoded); got != test.want {
				t.Fatalf("request JSON = %s, want %s", got, test.want)
			}
		})
	}
}
