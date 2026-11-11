package application

import (
	"context"
	"fmt"
	"time"

	"yap-algorithm/internal/ranking"
)

const (
	PersonalizationVersion = "personalized-v2"
	FeatureVersion         = "feed-features-v2"
	PolicyVersion          = "hybrid-v2"
)

type RefreshJob struct{ UserID, Lease string }
type VisualSource struct {
	PhotoID string `json:"photo_id"`
	SHA     string `json:"sha"`
}
type ProfileInput struct {
	Epoch            int
	Revision         int64
	At               time.Time
	Evidence         []ranking.Evidence
	NeedsEmbeddings  bool
	VisualCandidates []ranking.Candidate
	VisualEvidence   []ranking.Evidence
	VisualSources    []VisualSource
}
type RecommendationResult struct {
	Input          ProfileInput
	Profile        []ranking.Interest
	Candidates     []ranking.Candidate
	FeatureVersion string
	PolicyVersion  string
}

type RecommendationRepository interface {
	ClaimRefresh(context.Context) (RefreshJob, bool, error)
	LoadProfile(context.Context, RefreshJob) (ProfileInput, error)
	LoadCandidates(context.Context, RefreshJob, ProfileInput, []ranking.Interest) ([]ranking.Candidate, error)
	// Publish checks the lease and reset epoch and switches the complete snapshot
	// atomically. Feedback newer than Input.Revision stays queued for another pass.
	Publish(context.Context, RefreshJob, RecommendationResult) (bool, error)
	RetryRefresh(context.Context, RefreshJob) error
	CleanupRecommendations(context.Context) error
}

type Personalizer struct{ Repository RecommendationRepository }

func (p Personalizer) Once(ctx context.Context) (bool, error) {
	job, found, err := p.Repository.ClaimRefresh(ctx)
	if err != nil || !found {
		return false, err
	}
	input, err := p.Repository.LoadProfile(ctx, job)
	if err == nil {
		profile := ranking.BuildProfile(input.Evidence, input.At)
		var candidates []ranking.Candidate
		candidates, err = p.Repository.LoadCandidates(ctx, job, input, profile)
		if err == nil {
			ranking.Score(candidates, profile, input.At)
			// Geography is a small, bounded candidate source. Semantic interests
			// remain dominant; the chronological nearby tab is served by SQL.
			candidates = ranking.SelectForYou(candidates)
			if err = ctx.Err(); err == nil {
				_, err = p.Repository.Publish(ctx, job, RecommendationResult{Input: input, Profile: profile, Candidates: candidates, FeatureVersion: FeatureVersion, PolicyVersion: PolicyVersion})
			}
		}
	}
	if err != nil && ctx.Err() == nil {
		retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		retryErr := p.Repository.RetryRefresh(retryCtx, job)
		cancel()
		if retryErr != nil {
			return true, fmt.Errorf("personalize: %w; release: %w", err, retryErr)
		}
	}
	return true, err
}
