package main

import (
	"testing"
	"time"
)

func TestFailureDelay(t *testing.T) {
	var delay time.Duration
	for _, want := range []time.Duration{
		500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second,
	} {
		delay = nextFailureDelay(delay)
		if delay != want {
			t.Fatalf("retry delay = %s, want %s", delay, want)
		}
	}
}
