package postgres

import (
	"context"
	"encoding/json"
	"time"

	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

func (r *IndexRepository) ClaimBatch(ctx context.Context, kind string, limit int) ([]application.IndexJob, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	rows, err := r.pool.Query(ctx, "select id,lease::text,post_id::text,post_version,text from recsys.claim_index_batch($1::uuid,$2::text,$3::text,$4::integer)", domain.UUID(), r.model, kind, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []application.IndexJob
	for rows.Next() {
		var job application.IndexJob
		if err := rows.Scan(&job.ID, &job.Lease, &job.PostID, &job.Version, &job.Text); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (r *IndexRepository) CompleteBatch(ctx context.Context, completions []application.IndexCompletion) error {
	items := make([]map[string]any, 0, len(completions))
	for _, completion := range completions {
		var vector any
		if completion.Vector != nil {
			encoded, err := domain.Vector(completion.Vector)
			if err != nil {
				return err
			}
			vector = encoded
		}
		items = append(items, map[string]any{"id": completion.Job.ID, "lease": completion.Job.Lease, "vector": vector})
	}
	body, err := json.Marshal(items)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	_, err = r.pool.Exec(ctx, "select recsys.complete_index_batch($1::text::jsonb)", string(body))
	return err
}

// EnqueueBackfill scans one bounded UUID range. Existing live/leased jobs are
// untouched; operators explicitly continue using the returned cursor.
func (r *IndexRepository) EnqueueBackfill(ctx context.Context, after string, limit int) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var cursor any
	if after != "" {
		cursor = after
	}
	var raw []byte
	err := r.pool.QueryRow(ctx, "select recsys.enqueue_index_backfill($1::uuid,$2::integer)", cursor, limit).Scan(&raw)
	return raw, err
}

var _ application.BatchIndexRepository = (*IndexRepository)(nil)
