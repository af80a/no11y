package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type observation struct {
	Subject         string          `json:"subject"`
	ResponseSubject string          `json:"response_subject,omitempty"`
	Headers         nats.Header     `json:"headers,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	PayloadText     string          `json:"payload_text,omitempty"`
}

func main() {
	serverURL := flag.String("server", nats.DefaultURL, "NATS control server URL")
	creds := flag.String("creds", "", "optional NATS credentials file")
	user := flag.String("user", "", "optional NATS username")
	password := flag.String("password", "", "optional NATS password")
	subject := flag.String("subject", "$INS.ops.scraper", "control request subject")
	payload := flag.String("payload", "", "request payload")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if err := inspect(*serverURL, *creds, *user, *password, *subject, []byte(*payload), *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "control inspection failed:", err)
		os.Exit(1)
	}
}

func inspect(serverURL, creds, user, password, subject string, payload []byte, timeout time.Duration) error {
	options := []nats.Option{nats.Name("scraper-control-inspect")}
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

	response, err := nc.Request(subject, payload, timeout)
	if err != nil {
		return err
	}
	result := observation{
		Subject:         subject,
		ResponseSubject: response.Subject,
		Headers:         response.Header,
	}
	if json.Valid(response.Data) {
		result.Payload = append(result.Payload, response.Data...)
	} else {
		result.PayloadText = string(response.Data)
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
