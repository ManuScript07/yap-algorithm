package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/ranking"
)

type RecommendationRepository struct {
	pool  *pgxpool.Pool
	model string
}

// Recommendations shares the restricted pool with indexing. It never creates
// a second pool or introduces per-session state that could leak across users.
func (r *IndexRepository) Recommendations(ctx context.Context) (*RecommendationRepository, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var raw []byte
	if err := r.pool.QueryRow(ctx, "select recsys.personalization_contract()").Scan(&raw); err != nil {
		return nil, errors.New("Supabase personalization migration is missing or inaccessible")
	}
	var contract struct {
		Version  int    `json:"version"`
		Features string `json:"feature_version"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil || contract.Version != 1 || contract.Features != application.PersonalizationVersion {
		return nil, errors.New("personalization contract mismatch")
	}
	return &RecommendationRepository{pool: r.pool, model: r.model}, nil
}

func (r *RecommendationRepository) ClaimRefresh(ctx context.Context) (application.RefreshJob, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var job application.RefreshJob
	err := r.pool.QueryRow(ctx, "select user_id::text,lease::text from recsys.claim_refresh_job($1::uuid)", domain.UUID()).Scan(&job.UserID, &job.Lease)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, false, nil
	}
	return job, err == nil, err
}

func (r *RecommendationRepository) LoadProfile(ctx context.Context, job application.RefreshJob) (application.ProfileInput, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var raw []byte
	if err := r.pool.QueryRow(ctx, "select recsys.load_refresh_profile($1::uuid,$2::uuid,$3::text)", job.UserID, job.Lease, r.model).Scan(&raw); err != nil {
		return application.ProfileInput{}, err
	}
	var wire struct {
		Epoch           int       `json:"epoch"`
		Revision        int64     `json:"revision"`
		At              time.Time `json:"at"`
		NeedsEmbeddings bool      `json:"needs_embeddings"`
		Evidence        []struct {
			PostID  string    `json:"post_id"`
			Version int       `json:"post_version"`
			Vector  string    `json:"vector"`
			Weight  float64   `json:"weight"`
			At      time.Time `json:"at"`
		} `json:"evidence"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return application.ProfileInput{}, err
	}
	if wire.Epoch < 1 || wire.Revision < 1 || wire.At.IsZero() {
		return application.ProfileInput{}, errors.New("invalid profile input")
	}
	input := application.ProfileInput{Epoch: wire.Epoch, Revision: wire.Revision, At: wire.At, NeedsEmbeddings: wire.NeedsEmbeddings}
	for _, e := range wire.Evidence {
		input.Evidence = append(input.Evidence, ranking.Evidence{PostID: e.PostID, PostVersion: e.Version, Vector: domain.ParseVector(e.Vector), Weight: e.Weight, At: e.At})
	}
	return input, nil
}

func profileWire(profile []ranking.Interest) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(profile))
	for _, c := range profile {
		vector, err := domain.Vector(c.Vector)
		if err != nil {
			return nil, err
		}
		rows = append(rows, map[string]any{"vector": vector, "negative": c.Negative, "weight": c.Weight})
	}
	return rows, nil
}

func (r *RecommendationRepository) LoadCandidates(ctx context.Context, job application.RefreshJob, input application.ProfileInput, profile []ranking.Interest) ([]ranking.Candidate, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	profiles, err := profileWire(profile)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(profiles)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if err := r.pool.QueryRow(ctx, "select recsys.load_refresh_candidates($1::uuid,$2::uuid,$3::integer,$4::text,$5::text::jsonb)", job.UserID, job.Lease, input.Epoch, r.model, string(encoded)).Scan(&raw); err != nil {
		return nil, err
	}
	var wire []struct {
		ID          string    `json:"id"`
		Author      string    `json:"author_id"`
		Version     int       `json:"version"`
		At          time.Time `json:"created_at"`
		Vector      string    `json:"vector"`
		Sources     []string  `json:"sources"`
		Social      bool      `json:"social"`
		Impressions int       `json:"impressions"`
		Likes       int       `json:"likes"`
		Seen        bool      `json:"seen"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	if len(wire) > 1200 {
		return nil, errors.New("candidate budget exceeded")
	}
	rows := make([]ranking.Candidate, 0, len(wire))
	for _, c := range wire {
		var vector []float64
		if c.Vector != "" {
			vector = domain.ParseVector(c.Vector)
		}
		rows = append(rows, ranking.Candidate{ID: c.ID, AuthorID: c.Author, PostVersion: c.Version, CreatedAt: c.At, Embedding: vector, Sources: c.Sources, Social: c.Social, Impressions: c.Impressions, Likes: c.Likes, Seen: c.Seen})
	}
	return rows, nil
}

func (r *RecommendationRepository) Publish(ctx context.Context, job application.RefreshJob, result application.RecommendationResult) (bool, error) {
	profiles, err := profileWire(result.Profile)
	if err != nil {
		return false, err
	}
	items := make([]map[string]any, 0, len(result.Candidates))
	for _, c := range result.Candidates {
		items = append(items, map[string]any{"post_id": c.ID, "post_version": c.PostVersion, "features": c.Features, "score": c.Score, "reasons": c.Reasons, "sources": c.Sources})
	}
	evidence := make([]map[string]any, 0, len(result.Input.Evidence))
	for _, e := range result.Input.Evidence {
		evidence = append(evidence, map[string]any{"post_id": e.PostID, "post_version": e.PostVersion})
	}
	body, err := json.Marshal(map[string]any{"epoch": result.Input.Epoch, "revision": result.Input.Revision, "at": result.Input.At, "needs_embeddings": result.Input.NeedsEmbeddings,
		"feature_version": result.FeatureVersion, "profile": profiles, "items": items, "evidence": evidence})
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var accepted bool
	err = r.pool.QueryRow(ctx, "select recsys.publish_recommendations($1::uuid,$2::uuid,$3::text::jsonb)", job.UserID, job.Lease, string(body)).Scan(&accepted)
	return accepted, err
}

func (r *RecommendationRepository) RetryRefresh(ctx context.Context, job application.RefreshJob) error {
	_, err := r.pool.Exec(ctx, "select recsys.retry_refresh_job($1::uuid,$2::uuid)", job.UserID, job.Lease)
	return err
}
func (r *RecommendationRepository) CleanupRecommendations(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	// Small autocommitted batches make forward progress even when the overall
	// maintenance budget expires. Capacity must exceed the production arrival
	// rate; one tiny batch per minute would grow an unbounded expired backlog.
	for batch := 0; batch < 256; batch++ {
		var removed int
		if err := r.pool.QueryRow(ctx, "select recsys.cleanup_recommendations()").Scan(&removed); err != nil {
			return err
		}
		if removed == 0 {
			return nil
		}
	}
	return nil
}

var _ application.RecommendationRepository = (*RecommendationRepository)(nil)

func (r *RecommendationRepository) RetryFailed(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var recovered int
	err := r.pool.QueryRow(ctx, "select recsys.retry_failed_recommendation_jobs()").Scan(&recovered)
	if err != nil {
		return 0, errors.New("cannot reschedule failed recommendation jobs")
	}
	return recovered, nil
}
