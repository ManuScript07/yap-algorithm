package ranking

import (
	"math"
	"sort"
	"time"

	"yap-algorithm/internal/domain"
)

// Evidence represents bounded, authoritative state, not the number of times
// a client repeated an event. The repository excludes obsolete epochs/versions.
type Evidence struct {
	PostID      string
	PostVersion int
	Vector      []float64
	Weight      float64
	At          time.Time
}

// BuildProfile keeps up to four independent topics of each sign. When all
// slots are occupied, unrelated weak evidence cannot dilute a stronger topic.
func BuildProfile(evidence []Evidence, now time.Time) []Interest {
	type weighted struct {
		Evidence
		mass float64
	}
	items := make([]weighted, 0, len(evidence))
	for _, e := range evidence {
		if math.IsNaN(e.Weight) || math.IsInf(e.Weight, 0) || e.Weight == 0 {
			continue
		}
		v, err := domain.NormalizeVector(e.Vector)
		if err != nil {
			continue
		}
		age := math.Max(0, now.Sub(e.At).Hours())
		if age > 30*24 {
			continue
		}
		e.Vector = v
		items = append(items, weighted{e, math.Min(5, math.Abs(e.Weight)) * math.Exp(-age/(24*7))})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].mass == items[j].mass {
			return items[i].PostID < items[j].PostID
		}
		return items[i].mass > items[j].mass
	})
	clusters := make([]Interest, 0, 8)
	for _, e := range items {
		negative := e.Weight < 0
		best, count, similar := -1, 0, -2.0
		for i, c := range clusters {
			if c.Negative == negative {
				count++
				if sim := domain.Cosine(e.Vector, c.Vector); sim > similar {
					best, similar = i, sim
				}
			}
		}
		if best >= 0 && similar >= .82 {
			c := &clusters[best]
			v := make([]float64, len(e.Vector))
			for i := range v {
				v[i] = c.Vector[i]*c.Weight + e.Vector[i]*e.mass
			}
			if normalized, err := domain.NormalizeVector(v); err == nil {
				c.Vector = normalized
				c.Weight += e.mass
			}
		} else if count < 4 {
			clusters = append(clusters, Interest{Vector: e.Vector, Weight: e.mass, UpdatedAt: now, Negative: negative})
		}
	}
	return clusters
}
