package noop

import (
	"context"
	"log/slog"
	"testing"
)

func TestPrepareReturnsDiscardedPayloadCount(t *testing.T) {
	count, err := Prepare(context.Background(), slog.Default(), nil, "scrape.1.varz", 1, [][]byte{{1}, {2}}, 500)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}
