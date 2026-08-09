package main

import (
	"strings"
	"testing"

	"github.com/johndoe/nats-scraper/runner"
)

func TestApplyEndpointOverrides(t *testing.T) {
	config := runner.DefaultConfig
	if err := applyEndpointOverrides(&config, "connz, raftz", "varz=2,jsz=4"); err != nil {
		t.Fatal(err)
	}
	if !config.Connz.Disabled || !config.Raftz.Disabled {
		t.Fatalf("disabled overrides were not applied: connz=%t raftz=%t", config.Connz.Disabled, config.Raftz.Disabled)
	}
	if config.Varz.Frequency != 2 || config.Jsz.Frequency != 4 {
		t.Fatalf("frequency overrides were not applied: varz=%d jsz=%d", config.Varz.Frequency, config.Jsz.Frequency)
	}
}

func TestApplyEndpointOverridesRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		disabled    string
		frequencies string
		want        string
	}{
		{disabled: "unknown", want: `unknown disabled endpoint "unknown"`},
		{frequencies: "varz", want: `invalid endpoint frequency "varz"`},
		{frequencies: "varz=0", want: `invalid endpoint frequency "varz=0"`},
		{frequencies: "unknown=1", want: `unknown frequency endpoint "unknown"`},
	}
	for _, test := range tests {
		config := runner.DefaultConfig
		err := applyEndpointOverrides(&config, test.disabled, test.frequencies)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("applyEndpointOverrides(%q, %q) error = %v, want containing %q", test.disabled, test.frequencies, err, test.want)
		}
	}
}
