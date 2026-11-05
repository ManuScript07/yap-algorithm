package tests

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
)

type failingSoakTransport struct{ err error }

func (f failingSoakTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, f.err }

func TestLocalHTTPFailurePreservesTimeoutAndRedactsDetails(t *testing.T) {
	for _, test := range []struct {
		err     error
		timeout bool
	}{{context.DeadlineExceeded, true}, {errors.New("sensitive transport payload"), false}} {
		client := soakHTTP{"http://127.0.0.1", "test-key", &http.Client{Transport: failingSoakTransport{test.err}}}
		_, _, err := client.rpc("test-token", "get_for_you_feed", map[string]any{})
		var failure net.Error
		if !errors.As(err, &failure) || failure.Timeout() != test.timeout {
			t.Fatalf("timeout category lost: %v", err)
		}
		if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "http:") {
			t.Fatal("transport error exposed details")
		}
	}
}
