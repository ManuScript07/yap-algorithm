package dataset

import "testing"

func TestCaptureConnectionBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, dsn  string
		remote, ok bool
	}{
		{"local", "postgres://exporter@127.0.0.1:54322/postgres?sslmode=disable", false, true},
		{"verified remote", "postgres://exporter@db.example.test:5432/postgres?sslmode=verify-full", true, true},
		{"remote is opt-in", "postgres://exporter@db.example.test:5432/postgres?sslmode=verify-full", false, false},
		{"encryption alone", "postgres://exporter@db.example.test:5432/postgres?sslmode=require", true, false},
		{"no TLS fallback", "postgres://exporter@db.example.test:5432/postgres?sslmode=prefer", true, false},
		{"query override", "postgres://exporter@127.0.0.1:54322/postgres?sslmode=disable&host=elsewhere", false, false},
		{"ambiguous mode", "postgres://exporter@127.0.0.1:54322/postgres?sslmode=disable&sslmode=require", false, false},
		{"unrelated local db", "postgres://exporter@127.0.0.1:5432/postgres?sslmode=disable", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := ConnectionConfig(tc.dsn, tc.remote)
			if (err == nil) != tc.ok {
				t.Fatalf("allowed=%v, want %v: %v", err == nil, tc.ok, err)
			}
			if tc.ok && tc.remote && (config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify || len(config.Fallbacks) > 0) {
				t.Fatal("remote connection permits an unverified fallback")
			}
		})
	}
}
