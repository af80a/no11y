package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	scraper "github.com/johndoe/nats-scraper"
	"github.com/nats-io/nats.go"
)

type observation struct {
	Index          int             `json:"index"`
	Subject        string          `json:"subject"`
	HasReply       bool            `json:"has_reply"`
	AcceptEncoding string          `json:"accept_encoding,omitempty"`
	PayloadBytes   int             `json:"payload_bytes"`
	PayloadSHA256  string          `json:"payload_sha256"`
	Options        json.RawMessage `json:"options,omitempty"`
}

func main() {
	serverURL := flag.String("server", nats.DefaultURL, "NATS system server URL")
	creds := flag.String("creds", "", "optional NATS credentials file")
	user := flag.String("user", "", "optional NATS username")
	password := flag.String("password", "", "optional NATS password")
	timeout := flag.Duration("timeout", 2*time.Minute, "maximum time to observe two scrape starts")
	showOptions := flag.Bool("show-options", false, "include request JSON; may reveal account or filter names")
	flag.Parse()

	if err := inspect(*serverURL, *creds, *user, *password, *timeout, *showOptions); err != nil {
		fmt.Fprintln(os.Stderr, "request inspection failed:", err)
		os.Exit(1)
	}
}

func inspect(serverURL, creds, user, password string, timeout time.Duration, showOptions bool) error {
	options := []nats.Option{nats.Name("scraper-request-inspect")}
	if creds != "" {
		options = append(options, nats.UserCredentials(creds))
	}
	if user != "" || password != "" {
		options = append(options, nats.UserInfo(user, password))
	}
	nc, err := nats.Connect(serverURL, options...)
	if err != nil {
		return err
	}
	defer nc.Close()

	subscription, err := nc.SubscribeSync("$SYS.REQ.>")
	if err != nil {
		return err
	}
	if err := nc.Flush(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	encoder := json.NewEncoder(os.Stdout)
	var candidateDiscovery *nats.Msg
	capturing := false
	index := 0
	for {
		message, err := subscription.NextMsgWithContext(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("capture timed out after %s", timeout)
			}
			return err
		}
		if !isScraperRequest(message.Subject) {
			continue
		}
		if capturing {
			if message.Subject == scraper.ServerPingSubject {
				return nil
			}
			index++
			if err := encodeObservation(encoder, index, message, showOptions); err != nil {
				return err
			}
			continue
		}

		if candidateDiscovery == nil {
			if message.Subject == scraper.ServerPingSubject {
				candidateDiscovery = message
			}
			continue
		}
		if message.Subject != scraper.ServerPingSubject {
			candidateDiscovery = nil
			continue
		}

		index++
		if err := encodeObservation(encoder, index, candidateDiscovery, showOptions); err != nil {
			return err
		}
		index++
		if err := encodeObservation(encoder, index, message, showOptions); err != nil {
			return err
		}
		capturing = true
	}
}

func isScraperRequest(subject string) bool {
	return subject == scraper.ServerPingSubject ||
		subject == scraper.AccountStatzSubject ||
		strings.HasPrefix(subject, "$SYS.REQ.SERVER.")
}

func encodeObservation(encoder *json.Encoder, index int, message *nats.Msg, showOptions bool) error {
	digest := sha256.Sum256(message.Data)
	var requestOptions json.RawMessage
	if showOptions && json.Valid(message.Data) {
		requestOptions = append(requestOptions, message.Data...)
	}
	return encoder.Encode(observation{
		Index:          index,
		Subject:        message.Subject,
		HasReply:       message.Reply != "",
		AcceptEncoding: message.Header.Get("Accept-Encoding"),
		PayloadBytes:   len(message.Data),
		PayloadSHA256:  hex.EncodeToString(digest[:]),
		Options:        requestOptions,
	})
}
