package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"yap-algorithm/internal/api"
	"yap-algorithm/internal/cursor"
	"yap-algorithm/internal/model"
	"yap-algorithm/internal/store"
	"yap-algorithm/internal/worker"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func run() error {
	role := flag.String("role", "api", "api or worker")
	once := flag.Bool("once", false, "worker: drain available jobs then stop")
	reindex := flag.Bool("reindex", false, "worker: schedule missing or outdated embeddings and stop")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	revision := env("EMBEDDING_MODEL", "intfloat/multilingual-e5-small@614241f622f53c4eeff9890bdc4f31cfecc418b3:onnx-int8:query")
	s, e := store.Open(ctx, dsn, revision)
	if e != nil {
		return e
	}
	defer s.Pool.Close()
	if !s.Fixture(ctx) {
		return errors.New("legacy prototype requires the isolated fixture database; use cmd/worker for YapChat Supabase")
	}
	var unsafe bool
	if e = s.Pool.QueryRow(ctx, "select rolsuper or rolbypassrls from pg_roles where rolname=current_user").Scan(&unsafe); e != nil {
		return e
	}
	if unsafe {
		return errors.New("runtime requires a restricted database role, not superuser/BYPASSRLS")
	}
	m := model.New(env("MODEL_URL", "http://127.0.0.1:8091"), revision)
	if *role == "worker" {
		w := worker.Worker{Store: s, Model: m}
		if *reindex {
			n, e := w.Reindex(ctx)
			slog.Info("reindex_scheduled", "jobs", n)
			return e
		}
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		cleanup := time.NewTicker(time.Hour)
		defer cleanup.Stop()
		for {
			jobctx, cancel := context.WithTimeout(ctx, 25*time.Second)
			worked, e := w.Once(jobctx)
			cancel()
			if e != nil {
				slog.Warn("job_failed", "error", e)
			}
			if *once && !worked {
				return e
			}
			if worked {
				continue
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			case <-cleanup.C:
				if e := w.Cleanup(ctx); e != nil {
					slog.Warn("cleanup_failed", "error", e)
				}
			}
		}
	}
	if *role != "api" {
		return errors.New("unknown role")
	}
	address := env("LISTEN_ADDR", "127.0.0.1:8090")
	key := os.Getenv("CURSOR_SECRET")
	if len(key) < 32 {
		return errors.New("CURSOR_SECRET must have at least 32 bytes")
	}
	var auth api.Authenticator
	if env("AUTH_MODE", "supabase") == "local_fixture" {
		host, _, e := net.SplitHostPort(address)
		if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || !s.Fixture(ctx) {
			return errors.New("local fixture auth requires a loopback listener and the isolated fixture database")
		}
		auth = api.DevAuth{}
		slog.Warn("local_fixture_auth_enabled")
	} else if env("AUTH_MODE", "supabase") == "supabase" {
		url, key := os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_PUBLISHABLE_KEY")
		if url == "" || key == "" {
			return errors.New("Supabase URL and publishable key are required")
		}
		auth = api.SupabaseAuth{URL: url, Key: key, Client: &http.Client{Timeout: 3 * time.Second}}
	} else {
		return errors.New("unknown AUTH_MODE")
	}
	a := &api.API{Store: s, Auth: auth, Cursor: cursor.Codec{Key: []byte(key)}}
	if env("LEARNED_RANKING", "false") == "true" {
		a.Scorer = m
	}
	server := &http.Server{Addr: address, Handler: a.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("listening", "address", address, "role", *role)
	e = server.ListenAndServe()
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if e := run(); e != nil {
		slog.Error("stopped", "error", e)
		os.Exit(1)
	}
}
