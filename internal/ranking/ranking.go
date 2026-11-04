package ranking

import (
	"math"
	"sort"
	"time"
	"yap-algorithm/internal/domain"
)

const Version = "hybrid-v1"

// Feature order is a versioned contract with the model service and training export.
var FeatureNames = []string{"semantic", "social", "quality", "freshness", "negative_similarity", "low_exposure", "nearby", "seen"}

type Candidate struct {
	ID, AuthorID                string
	PostVersion                 int
	CreatedAt                   time.Time
	Embedding                   []float64
	Sources                     []string
	Social, Nearby              bool
	Impressions, Likes, Reposts int
	Seen                        bool
	Features                    []float64
	Score                       float64
	Reasons                     []string
}
type Interest struct {
	Vector    []float64
	Weight    float64
	UpdatedAt time.Time
	Negative  bool
}

func Features(c Candidate, interests []Interest, now time.Time) []float64 {
	positive, negative := 0.0, 0.0
	for _, p := range interests {
		sim := math.Max(0, (domain.Cosine(c.Embedding, p.Vector)-0.65)/0.35)
		decay := math.Exp(-math.Max(0, now.Sub(p.UpdatedAt).Hours()) / (24 * 7))
		confidence := math.Min(1, p.Weight/3) * decay
		if p.Negative {
			negative = math.Max(negative, sim*confidence)
		} else {
			positive = math.Max(positive, sim*confidence)
		}
	}
	age := math.Max(0, now.Sub(c.CreatedAt).Hours())
	quality := math.Min(1, (float64(c.Likes)+2*float64(c.Reposts)+1)/(float64(c.Impressions)+20))
	return []float64{positive, boolf(c.Social), quality, math.Exp(-age / 48), negative, 1 / math.Sqrt(1+float64(c.Impressions)/20), boolf(c.Nearby), boolf(c.Seen)}
}
func Score(candidates []Candidate, interests []Interest, now time.Time) {
	for i := range candidates {
		c := &candidates[i]
		c.Features = Features(*c, interests, now)
		f := c.Features
		c.Score = 0.46*f[0] + 0.13*f[1] + 0.10*f[2] + 0.16*f[3] + 0.10*f[5] + 0.05*f[6] - 0.50*f[4] - 0.18*f[7]
		c.Reasons = []string{}
		if f[0] > .2 {
			c.Reasons = append(c.Reasons, "similar_interests")
		}
		if c.Social {
			c.Reasons = append(c.Reasons, "social_connection")
		}
		if c.Nearby {
			c.Reasons = append(c.Reasons, "nearby")
		}
		if c.Impressions < 20 {
			c.Reasons = append(c.Reasons, "discover_post")
		}
	}
}

// Rerank uses MMR and author saturation. Ties are deterministic across retries.
// Exploration is reserved for eligible, low-exposure candidates, never bypasses access.
func Rerank(in []Candidate, limit int, exploreEvery int) []Candidate {
	pool := append([]Candidate(nil), in...)
	sort.Slice(pool, func(i, j int) bool {
		if pool[i].Score == pool[j].Score {
			return pool[i].ID < pool[j].ID
		}
		return pool[i].Score > pool[j].Score
	})
	if limit > len(pool) {
		limit = len(pool)
	}
	out := make([]Candidate, 0, limit)
	authors := map[string]int{}
	maxSimilarity := make([]float64, len(pool))
	for len(out) < limit {
		best, bestValue := -1, math.Inf(-1)
		explore := exploreEvery > 0 && (len(out)+1)%exploreEvery == 0
		for i, c := range pool {
			if c.ID == "" {
				continue
			}
			v := c.Score - 0.15*maxSimilarity[i] - 0.12*float64(authors[c.AuthorID])
			if len(out) > 0 && out[len(out)-1].AuthorID == c.AuthorID {
				v -= 0.25
			}
			if explore && c.Impressions < 20 {
				v += 0.20
			}
			if v > bestValue {
				best, bestValue = i, v
			}
		}
		if best < 0 {
			break
		}
		c := pool[best]
		if explore && c.Impressions < 20 {
			c.Reasons = append(c.Reasons, "exploration")
		}
		out = append(out, c)
		authors[c.AuthorID]++
		pool[best].ID = ""
		for i := range pool {
			if pool[i].ID != "" {
				maxSimilarity[i] = math.Max(maxSimilarity[i], domain.Cosine(pool[i].Embedding, c.Embedding))
			}
		}
	}
	return out
}
func boolf(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
