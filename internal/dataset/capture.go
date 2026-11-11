package dataset

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
)

const Schema = "yap-feedback-capture-v1"

type Options struct {
	Cohort     string
	Output     string
	PageSize   int
	MaxRows    int
	Timeout    time.Duration
	BinaryHash string
}

type Manifest struct {
	Schema       string          `json:"schema"`
	Database     json.RawMessage `json:"database"`
	CompletedAt  time.Time       `json:"completed_at"`
	Rows         int             `json:"rows"`
	Excluded     int             `json:"excluded_at_source"`
	LastSequence int64           `json:"last_sequence"`
	FactsHash    string          `json:"facts_sha256"`
	BinaryHash   string          `json:"binary_sha256"`
}

func ConnectionConfig(dsn string, remote bool) (*pgx.ConnConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() == "" || u.User == nil || u.User.Username() == "" ||
		u.Fragment != "" || len(u.Path) < 2 || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, errors.New("set YAP_DATASET_DSN to an explicit PostgreSQL URL")
	}
	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, errors.New("invalid exporter connection options")
	}
	for key, values := range params {
		if len(values) != 1 || (key != "sslmode" && key != "sslrootcert") {
			return nil, errors.New("unexpected exporter connection option")
		}
	}
	if remote {
		if params.Get("sslmode") != "verify-full" {
			return nil, errors.New("remote capture requires sslmode=verify-full")
		}
	} else if u.Hostname() != "127.0.0.1" || u.Port() != "54322" || u.Path != "/postgres" || params.Get("sslmode") != "disable" {
		return nil, errors.New("local capture requires 127.0.0.1:54322 and sslmode=disable")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil || config.Host != u.Hostname() || len(config.Fallbacks) != 0 ||
		(remote && (config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify ||
			config.TLSConfig.ServerName != config.Host)) {
		return nil, errors.New("invalid exporter connection configuration")
	}
	config.RuntimeParams = map[string]string{"application_name": "yap-dataset-capture", "timezone": "UTC"}
	config.ConnectTimeout = 5 * time.Second
	return config, nil
}

func Capture(ctx context.Context, conn *pgx.Conn, opts Options) (Manifest, error) {
	m := Manifest{Schema: Schema, BinaryHash: opts.BinaryHash}
	if opts.Cohort == "" || opts.Output == "" || opts.PageSize < 1 || opts.PageSize > 500 ||
		opts.MaxRows < 1 || opts.MaxRows > 250000 || opts.Timeout <= 0 || opts.Timeout > 60*time.Second {
		return m, errors.New("invalid capture options")
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	if _, err := os.Lstat(opts.Output); !os.IsNotExist(err) {
		return m, errors.New("capture output must not already exist")
	}
	dir, err := os.MkdirTemp(filepath.Dir(opts.Output), ".capture-")
	if err != nil {
		return m, err
	}
	defer os.RemoveAll(dir)
	file, err := os.OpenFile(filepath.Join(dir, "facts.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return m, err
	}
	defer file.Close()
	hash := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hash))

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return m, err
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		_ = tx.Rollback(cleanup)
	}()
	if _, err := tx.Exec(ctx, "set local statement_timeout='5s'; set local idle_in_transaction_session_timeout='8s'"); err != nil {
		return m, err
	}
	var restricted bool
	if err := tx.QueryRow(ctx, `select pg_has_role(current_user,'yap_recommendation_exporter','USAGE')
        and exists(select 1 from pg_roles where rolname=current_user and rolcanlogin and not
          (rolsuper or rolcreatedb or rolcreaterole or rolreplication or rolbypassrls))
		and not exists(select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where (n.nspname,c.relname) in (('recsys','feedback_facts'),('feed','posts'),('recsys_dataset','members'))
          and has_table_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'))`).Scan(&restricted); err != nil {
		return m, err
	}
	if !restricted {
		return m, errors.New("capture requires a dedicated restricted exporter login")
	}
	if err := tx.QueryRow(ctx, "select recsys_dataset.capture_info($1::uuid)", opts.Cohort).Scan(&m.Database); err != nil {
		return m, fmt.Errorf("capture metadata: %w", err)
	}
	var size int
	for {
		rows, err := tx.Query(ctx, "select sequence,fact from recsys_dataset.capture_page($1::uuid,$2,$3)", opts.Cohort, m.LastSequence, opts.PageSize)
		if err != nil {
			return m, fmt.Errorf("read capture page: %w", err)
		}
		n := 0
		for rows.Next() {
			var seq int64
			var fact json.RawMessage
			if err := rows.Scan(&seq, &fact); err != nil {
				rows.Close()
				return m, err
			}
			if seq <= m.LastSequence || m.Rows+m.Excluded == opts.MaxRows {
				rows.Close()
				return m, errors.New("capture sequence or row budget exceeded")
			}
			m.LastSequence = seq
			n++
			if len(fact) == 0 {
				m.Excluded++
				continue
			}
			size += len(fact) + 1
			if !json.Valid(fact) || len(fact) > 256*1024 || size > 256*1024*1024 {
				rows.Close()
				return m, errors.New("invalid capture row")
			}
			if _, err := writer.Write(fact); err != nil {
				rows.Close()
				return m, err
			}
			if err := writer.WriteByte('\n'); err != nil {
				rows.Close()
				return m, err
			}
			m.Rows++
		}
		if err := rows.Err(); err != nil {
			return m, fmt.Errorf("scan capture page: %w", err)
		}
		if n < opts.PageSize {
			break
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return m, err
	}
	if err := conn.QueryRow(ctx, "select clock_timestamp()").Scan(&m.CompletedAt); err != nil {
		return m, err
	}
	m.CompletedAt = m.CompletedAt.UTC()
	if err := writer.Flush(); err != nil {
		return m, err
	}
	if err := file.Sync(); err != nil {
		return m, err
	}
	if err := file.Close(); err != nil {
		return m, err
	}
	m.FactsHash = hex.EncodeToString(hash.Sum(nil))
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0600); err != nil {
		return m, err
	}
	if err := ctx.Err(); err != nil {
		return m, err
	}
	// Rename publishes both files as a unit. An existing nonempty capture
	// directory cannot be replaced by this rename.
	return m, os.Rename(dir, opts.Output)
}
