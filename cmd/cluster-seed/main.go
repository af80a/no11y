package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type result struct {
	Servers  int    `json:"servers"`
	Stream   string `json:"stream"`
	Consumer string `json:"consumer"`
	Messages int    `json:"messages"`
}

func main() {
	serverList := flag.String("servers", "nats://127.0.0.1:16221,nats://127.0.0.1:16222,nats://127.0.0.1:16223", "comma-separated NATS application-account URLs")
	user := flag.String("user", "", "optional NATS username")
	password := flag.String("password", "", "optional NATS password")
	streamName := flag.String("stream", "ORDERS", "replicated stream name")
	consumerName := flag.String("consumer", "WORKERS", "replicated durable consumer name")
	messageCount := flag.Int("messages", 12, "number of seed messages")
	hold := flag.Duration("hold", 0, "keep all node-local clients connected for this duration (0=until signal)")
	setupTimeout := flag.Duration("setup-timeout", 30*time.Second, "maximum cluster setup time")
	flag.Parse()

	if err := seed(*serverList, *user, *password, *streamName, *consumerName, *messageCount, *hold, *setupTimeout); err != nil {
		fmt.Fprintln(os.Stderr, "cluster seed failed:", err)
		os.Exit(1)
	}
}

func seed(serverList, user, password, streamName, consumerName string, messageCount int, hold, setupTimeout time.Duration) error {
	servers := splitServers(serverList)
	if len(servers) == 0 {
		return fmt.Errorf("at least one server URL is required")
	}
	if messageCount < 0 {
		return fmt.Errorf("message count must not be negative")
	}

	connections := make([]*nats.Conn, 0, len(servers))
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	for index, serverURL := range servers {
		options := []nats.Option{nats.Name(fmt.Sprintf("scraper-cluster-seed-%d", index+1))}
		if user != "" || password != "" {
			options = append(options, nats.UserInfo(user, password))
		}
		connection, err := nats.Connect(serverURL, options...)
		if err != nil {
			return fmt.Errorf("connect node %d: %w", index+1, err)
		}
		connections = append(connections, connection)
		if _, err := connection.Subscribe(fmt.Sprintf("cluster.probe.node%d", index+1), func(*nats.Msg) {}); err != nil {
			return fmt.Errorf("subscribe node %d: %w", index+1, err)
		}
		if err := connection.Flush(); err != nil {
			return fmt.Errorf("flush node %d: %w", index+1, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()
	js, err := jetstream.New(connections[0])
	if err != nil {
		return err
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"orders.>"},
		Storage:  jetstream.FileStorage,
		Replicas: len(servers),
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		Replicas:  len(servers),
	}); err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	for index := 0; index < messageCount; index++ {
		if _, err := js.Publish(ctx, "orders.created", []byte(fmt.Sprintf("order-%03d", index+1))); err != nil {
			return fmt.Errorf("publish message %d: %w", index+1, err)
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(result{
		Servers:  len(servers),
		Stream:   streamName,
		Consumer: consumerName,
		Messages: messageCount,
	}); err != nil {
		return err
	}

	waitContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if hold > 0 {
		timer := time.NewTimer(hold)
		defer timer.Stop()
		select {
		case <-waitContext.Done():
		case <-timer.C:
		}
		return nil
	}
	<-waitContext.Done()
	return nil
}

func splitServers(value string) []string {
	var servers []string
	for _, server := range strings.Split(value, ",") {
		if server = strings.TrimSpace(server); server != "" {
			servers = append(servers, server)
		}
	}
	return servers
}
