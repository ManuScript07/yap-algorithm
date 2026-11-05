package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
)

type MediaRepository struct{ pool *pgxpool.Pool }

func OpenMediaRepository(ctx context.Context, dsn string, connections int32) (*MediaRepository, error) {
	cfg, err := connectionConfig(dsn, connections)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("cannot create media database pool")
	}
	repo := &MediaRepository{pool: pool}
	if err := repo.verify(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return repo, nil
}

func (r *MediaRepository) Close() { r.pool.Close() }

func (r *MediaRepository) verify(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var allowed bool
	err := r.pool.QueryRow(ctx, `select not(rolsuper or rolbypassrls or rolcreaterole or rolcreatedb)
 and pg_has_role(current_user,'yap_feed_media_worker','USAGE')
 and not exists(select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
   where ((n.nspname='feed'and c.relname='posts')or(n.nspname='public'and c.relname='profiles'))
     and has_table_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,DELETE'))
 from pg_roles where rolname=current_user`).Scan(&allowed)
	if err != nil || !allowed {
		return errors.New("media worker requires its restricted database role")
	}
	var raw []byte
	if err := r.pool.QueryRow(ctx, "select media.contract()").Scan(&raw); err != nil {
		return errors.New("media migration missing or inaccessible")
	}
	var contract struct {
		Version   int    `json:"version"`
		Processor string `json:"processor"`
		MaxBytes  int    `json:"max_bytes"`
	}
	if json.Unmarshal(raw, &contract) != nil || contract.Version != 1 || contract.Processor != "jpeg-v1" || contract.MaxBytes != application.MaxPhotoBytes {
		return errors.New("media processor contract mismatch")
	}
	return nil
}

func (r *MediaRepository) read(ctx context.Context, result any, sql string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var raw []byte
	if err := r.pool.QueryRow(ctx, sql, args...).Scan(&raw); err != nil {
		return err
	}
	if len(raw) == 0 {
		raw = []byte("null")
	}
	return json.Unmarshal(raw, result)
}

func (r *MediaRepository) exec(ctx context.Context, sql string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	_, err := r.pool.Exec(ctx, sql, args...)
	return err
}

func (r *MediaRepository) ClaimPhoto(ctx context.Context) (application.PhotoJob, bool, error) {
	var job *application.PhotoJob
	err := r.read(ctx, &job, "select media.claim_photo($1::uuid)", domain.UUID())
	if job == nil {
		return application.PhotoJob{}, false, err
	}
	return *job, true, err
}

func (r *MediaRepository) CompletePhoto(ctx context.Context, job application.PhotoJob, photo application.PreparedPhoto) (bool, error) {
	body, err := json.Marshal(map[string]any{"width": photo.Width, "height": photo.Height, "image_bytes": len(photo.Image),
		"thumbnail_bytes": len(photo.Thumbnail), "sha256": photo.SHA256})
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var accepted bool
	err = r.pool.QueryRow(ctx, "select media.complete_photo($1::uuid,$2::uuid,$3::text::jsonb)", job.ID, job.Lease, string(body)).Scan(&accepted)
	return accepted, err
}

func (r *MediaRepository) FailPhoto(ctx context.Context, job application.PhotoJob, invalid bool) error {
	return r.exec(ctx, "select media.fail_photo($1::uuid,$2::uuid,$3)", job.ID, job.Lease, invalid)
}

func (r *MediaRepository) ClaimCleanup(ctx context.Context) (application.MediaCleanupJob, bool, error) {
	var job *application.MediaCleanupJob
	err := r.read(ctx, &job, "select media.claim_cleanup($1::uuid)", domain.UUID())
	if job == nil {
		return application.MediaCleanupJob{}, false, err
	}
	return *job, true, err
}

func (r *MediaRepository) CompleteCleanup(ctx context.Context, job application.MediaCleanupJob, succeeded bool) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var acknowledged bool
	err := r.pool.QueryRow(ctx, "select media.complete_cleanup($1,$2,$3::uuid,$4)", job.Bucket, job.Path, job.Lease, succeeded).Scan(&acknowledged)
	if err == nil && succeeded && !acknowledged {
		return errors.New("physical removal not confirmed or cleanup lease expired")
	}
	return err
}

func (r *MediaRepository) Maintenance(ctx context.Context, retry bool) error {
	return r.exec(ctx, "select media.maintenance($1)", retry)
}

var _ application.MediaRepository = (*MediaRepository)(nil)
