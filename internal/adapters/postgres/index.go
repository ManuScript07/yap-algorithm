package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

type IndexRepository struct {
	pool  *pgxpool.Pool
	model string
}

func connectionConfig(dsn string, connections int32) (*pgxpool.Config, error) {
	if connections < 1 || connections > 16 {
		return nil, errors.New("database pool must contain 1..16 connections")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	}
	host := cfg.ConnConfig.Host
	address := net.ParseIP(host)
	local := host == "localhost" || (address != nil && address.IsLoopback())
	if !local {
		// Reject prefer/require/verify-ca, including fallback connections. Remote
		// deployments must verify both the certificate chain and hostname.
		if cfg.ConnConfig.TLSConfig == nil || cfg.ConnConfig.TLSConfig.InsecureSkipVerify || cfg.ConnConfig.TLSConfig.ServerName == "" || len(cfg.ConnConfig.Fallbacks) != 0 {
			return nil, errors.New("remote DATABASE_URL requires sslmode=verify-full")
		}
	}
	for _, fallback := range cfg.ConnConfig.Fallbacks {
		address := net.ParseIP(fallback.Host)
		local := fallback.Host == "localhost" || (address != nil && address.IsLoopback())
		if !local && (fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify || fallback.TLSConfig.ServerName == "") {
			return nil, errors.New("remote database fallback requires sslmode=verify-full")
		}
	}
	cfg.MaxConns = connections
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	// No server-side prepared statements or session identity: each operation is
	// a single transactional function, compatible with transaction pooling.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	return cfg, nil
}

func OpenIndexRepository(ctx context.Context, dsn, model string, connections int32) (*IndexRepository, error) {
	cfg, err := connectionConfig(dsn, connections)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("cannot create database pool")
	}
	repo := &IndexRepository{pool: pool, model: model}
	if err := repo.verify(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return repo, nil
}

func (r *IndexRepository) Close() { r.pool.Close() }

func (r *IndexRepository) verify(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var unsafe, member bool
	err := r.pool.QueryRow(ctx, `select rolsuper or rolbypassrls or rolcreaterole or rolcreatedb,
 pg_has_role(current_user,'yap_recommendation_worker','USAGE')
 from pg_roles where rolname=current_user`).Scan(&unsafe, &member)
	if err != nil {
		return errors.New("cannot verify worker database role")
	}
	if unsafe || !member {
		return errors.New("worker requires a restricted yap_recommendation_worker role")
	}
	var raw []byte
	if err := r.pool.QueryRow(ctx, "select recsys.pipeline_contract()").Scan(&raw); err != nil {
		return errors.New("Supabase pipeline migration is missing or inaccessible")
	}
	var contract struct {
		Version int    `json:"version"`
		Model   string `json:"embedding_model"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil || contract.Version != 1 || contract.Model != r.model {
		return errors.New("Supabase pipeline contract/model mismatch")
	}
	var excessive bool
	if err := r.pool.QueryRow(ctx, `select coalesce(bool_or(has_table_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,DELETE')),true)
 from pg_class c join pg_namespace n on n.oid=c.relnamespace
 where (n.nspname='feed' and c.relname='posts') or (n.nspname='public' and c.relname='profiles')`).Scan(&excessive); err != nil || excessive {
		return errors.New("worker role must not have raw content/profile table privileges")
	}
	return nil
}

func (r *IndexRepository) Claim(ctx context.Context) (application.IndexJob, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var job application.IndexJob
	err := r.pool.QueryRow(ctx, "select id,lease::text,post_id::text,post_version,text from recsys.claim_index_job($1::uuid,$2::text)", domain.UUID(), r.model).
		Scan(&job.ID, &job.Lease, &job.PostID, &job.Version, &job.Text)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, false, nil
	}
	return job, err == nil, err
}

func (r *IndexRepository) Complete(ctx context.Context, job application.IndexJob, vector []float64) (bool, error) {
	var encoded any
	if vector != nil {
		value, err := domain.Vector(vector)
		if err != nil {
			return false, err
		}
		encoded = value
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var accepted bool
	err := r.pool.QueryRow(ctx, "select recsys.complete_index_job($1::bigint,$2::uuid,$3::text::extensions.vector)", job.ID, job.Lease, encoded).Scan(&accepted)
	return accepted, err
}

func (r *IndexRepository) Retry(ctx context.Context, job application.IndexJob) error {
	_, err := r.pool.Exec(ctx, "select recsys.retry_index_job($1::bigint,$2::uuid)", job.ID, job.Lease)
	return err
}

func (r *IndexRepository) Cleanup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	_, err := r.pool.Exec(ctx, "select recsys.cleanup_index_jobs()")
	if err != nil {
		return fmt.Errorf("cleanup index jobs: %w", err)
	}
	return nil
}

var _ application.IndexRepository = (*IndexRepository)(nil)
