package postgres

import (
	"context"
	"encoding/json"
	"time"

	"yap-algorithm/internal/application"
	"yap-algorithm/internal/ranking"
)

// WithVision opts in to the database's smaller pilot allowlist. The ordinary
// preparation path keeps working when this deployment option is absent.
func (r *RecommendationRepository) WithVision() *RecommendationRepository {
	copy := *r
	copy.vision = true
	return &copy
}

func (r *RecommendationRepository) loadVision(ctx context.Context, job application.RefreshJob, input *application.ProfileInput) {
	ctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	var raw []byte
	if err := r.pool.QueryRow(ctx, "select recsys.load_vision_candidates($1::uuid,$2::uuid,$3::integer)",
		job.UserID, job.Lease, input.Epoch).Scan(&raw); err != nil || len(raw) == 0 {
		return
	}
	var result struct {
		Items []struct {
			ID                string    `json:"id"`
			Author            string    `json:"author_id"`
			Version           int       `json:"version"`
			Created           time.Time `json:"created_at"`
			Similarity        float64   `json:"similarity"`
			Impressions       int       `json:"impressions"`
			Likes             int       `json:"likes"`
			Reposts           int       `json:"reposts"`
			AuthorImpressions int       `json:"author_impressions"`
			AuthorViewers     int       `json:"author_viewers"`
			Seen              bool      `json:"seen"`
		} `json:"items"`
		Evidence []struct {
			ID      string `json:"post_id"`
			Version int    `json:"post_version"`
		} `json:"evidence"`
		Fence []application.VisualSource `json:"fence"`
	}
	if json.Unmarshal(raw, &result) != nil || len(result.Items) > 32 || len(result.Evidence) > 24 || len(result.Fence) > 168 {
		return
	}
	for _, item := range result.Items {
		if item.ID == "" || item.Author == "" || item.Version < 1 || item.Similarity <= 0 || item.Similarity > 1 {
			continue
		}
		input.VisualCandidates = append(input.VisualCandidates, ranking.Candidate{
			ID: item.ID, AuthorID: item.Author, PostVersion: item.Version, CreatedAt: item.Created,
			Visual: item.Similarity, Sources: []string{"vision"}, Impressions: item.Impressions,
			Likes: item.Likes, Reposts: item.Reposts, AuthorImpressions: item.AuthorImpressions,
			AuthorViewers: item.AuthorViewers, HasAuthorExposure: true, Seen: item.Seen,
		})
	}
	for _, item := range result.Evidence {
		input.VisualEvidence = append(input.VisualEvidence, ranking.Evidence{PostID: item.ID, PostVersion: item.Version})
	}
	input.VisualSources = result.Fence
}

func mergeVision(base, visual []ranking.Candidate) []ranking.Candidate {
	if len(visual) == 0 {
		return base
	}
	index := make(map[string]int, len(base))
	for i := range base {
		index[base[i].ID] = i
	}
	for _, item := range visual {
		if i, found := index[item.ID]; found {
			// A concurrent caption edit supersedes the photo-only score.
			if base[i].PostVersion == item.PostVersion && len(base[i].Embedding) == 0 {
				base[i].Visual = item.Visual
				base[i].Sources = append(base[i].Sources, "vision")
			}
			continue
		}
		if len(base) >= 1200 {
			// Retain the established cap. At most 32 low-priority tail entries
			// can be displaced by the independent source.
			drop := len(base) - 1
			for drop >= 0 && base[drop].Visual > 0 {
				drop--
			}
			if drop < 0 {
				break
			}
			delete(index, base[drop].ID)
			base[drop] = item
			index[item.ID] = drop
		} else {
			index[item.ID] = len(base)
			base = append(base, item)
		}
	}
	return base
}
