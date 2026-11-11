package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"yap-algorithm/internal/dataset"
)

func executableHash() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func run() error {
	var opts dataset.Options
	flag.StringVar(&opts.Cohort, "cohort", "", "registered synthetic cohort UUID")
	flag.StringVar(&opts.Output, "output", "", "new directory inside private .local/output")
	flag.IntVar(&opts.PageSize, "page-size", 200, "SQL page size (1–500)")
	flag.IntVar(&opts.MaxRows, "max-rows", 50000, "maximum facts scanned")
	flag.DurationVar(&opts.Timeout, "timeout", 30*time.Second, "capture budget (up to 60s)")
	remote := flag.Bool("remote", false, "allow an explicit TLS-verified remote DSN")
	flag.Parse()
	var err error
	opts.BinaryHash, err = executableHash()
	if err != nil {
		return err
	}
	config, err := dataset.ConnectionConfig(os.Getenv("YAP_DATASET_DSN"), *remote)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return errors.New("could not connect as dataset exporter")
	}
	defer conn.Close(context.Background())
	m, err := dataset.Capture(ctx, conn, opts)
	if err != nil {
		var db *pgconn.PgError
		if errors.As(err, &db) {
			return fmt.Errorf("database rejected the capture (SQLSTATE %s)", db.Code)
		}
		return err
	}
	fmt.Printf("captured %d rows (%d filtered); facts SHA-256 %s\n", m.Rows, m.Excluded, m.FactsHash)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
