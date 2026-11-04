package postgres

import "testing"

func TestConnectionTransportPolicy(t *testing.T) {
	for _, tc := range []struct {
		dsn     string
		allowed bool
	}{
		{"postgres://local:test@127.0.0.1:54322/postgres?sslmode=disable", true},
		{"postgres://local:test@localhost:54322/postgres?sslmode=disable", true},
		{"postgres://local:test@db.example.com/postgres?sslmode=disable", false},
		{"postgres://local:test@db.example.com/postgres?sslmode=require", false},
		{"postgres://local:test@db.example.com/postgres?sslmode=verify-ca", false},
		{"postgres://local:test@db.example.com/postgres?sslmode=verify-full", true},
		{"postgres://local:test@localhost/postgres?host=localhost,db.example.com&sslmode=disable", false},
	} {
		_, err := connectionConfig(tc.dsn, 4)
		if (err == nil) != tc.allowed {
			t.Fatalf("transport policy allowed=%v, err=%v", tc.allowed, err)
		}
	}
	if _, err := connectionConfig("postgres://localhost/postgres", 1000); err == nil {
		t.Fatal("unbounded pool accepted")
	}
}
