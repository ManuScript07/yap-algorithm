package tests

import (
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

type supabaseTestTarget struct {
	apiURL, container, stateDir string
	port                        uint16
}

func localSupabaseTarget(t *testing.T) supabaseTestTarget {
	t.Helper()
	switch os.Getenv("YAP_SUPABASE_TEST_TARGET") {
	case "", "default":
		return supabaseTestTarget{"http://127.0.0.1:54321", "supabase_db_Yap_chat", "../.local", 54322}
	case "stage7":
		return supabaseTestTarget{"http://127.0.0.1:55321", "supabase_db_yap_stage7", "../.local/stage7-supabase", 55322}
	default:
		t.Fatal("unknown local Supabase test target")
		return supabaseTestTarget{}
	}
}

func (target supabaseTestTarget) acceptsPool(cfg *pgxpool.Config) bool {
	if cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.Port != target.port || cfg.ConnConfig.Database != "postgres" {
		return false
	}
	for _, fallback := range cfg.ConnConfig.Fallbacks {
		if fallback.Host != "127.0.0.1" || fallback.Port != target.port {
			return false
		}
	}
	return true
}

func (target supabaseTestTarget) acceptsURL(endpoint *url.URL) bool {
	return (endpoint.Scheme == "postgres" || endpoint.Scheme == "postgresql") && endpoint.Hostname() == "127.0.0.1" &&
		endpoint.Port() == strconv.Itoa(int(target.port)) && endpoint.Path == "/postgres" &&
		(endpoint.RawQuery == "" || endpoint.RawQuery == "sslmode=disable")
}

func (target supabaseTestTarget) workerConfig() string {
	return filepath.Join(target.stateDir, "supabase-worker.json")
}

func (target supabaseTestTarget) databaseAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(int(target.port)))
}
