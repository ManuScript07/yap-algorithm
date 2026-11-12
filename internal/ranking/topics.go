package ranking

import (
	"math"
	"time"
)

// TopicConfidence gives explicit topics their largest influence before the
// viewer has enough recent evidence to personalize from their own actions.
func TopicConfidence(evidence []Evidence, now time.Time) float64 {
	recent := make(map[string]struct{})
	for _, item := range evidence {
		age := now.Sub(item.At)
		// A follow alone currently contributes 1. Likes, comments, reposts,
		// and sufficient attention can count towards replacing onboarding.
		if item.PostID != "" && item.Weight >= 1.2 && age >= 0 && age <= 14*24*time.Hour {
			recent[item.PostID] = struct{}{}
		}
	}
	confidence := 0.70 / (1 + float64(len(recent))/4)
	if confidence < 0.15 {
		return 0
	}
	return confidence
}

// TopicRetrieval uses the existing bounded semantic source. Negative learned
// interests still suppress ranking; they have never seeded positive retrieval.
func TopicRetrieval(profile []Interest, vectors [][]float64, confidence float64) []Interest {
	if len(vectors) == 0 || confidence == 0 {
		return profile
	}
	const maxTopics, maxClusters = 4, 4
	result := make([]Interest, 0, maxTopics+maxClusters)
	for _, interest := range profile {
		if !interest.Negative && len(result) < maxClusters {
			result = append(result, interest)
		}
	}
	for i, vector := range vectors {
		if i == maxTopics {
			break
		}
		result = append(result, Interest{Vector: vector, Weight: confidence})
	}
	return result
}

// ApplyTopicPrior keeps explicit choices out of the learned clusters and uses
// the stronger semantic signal. A post cannot collect a boost per chosen topic.
func ApplyTopicPrior(candidates []Candidate, vectors [][]float64, confidence float64) {
	if len(vectors) == 0 || confidence == 0 {
		return
	}
	for i := range candidates {
		candidate := &candidates[i]
		if len(candidate.Embedding) == 0 || len(candidate.Features) != len(FeatureNames) {
			continue
		}
		best := 0.0
		for _, vector := range vectors {
			best = math.Max(best, semanticSimilarity(candidate.Embedding, vector)*confidence)
		}
		if best <= candidate.Features[semanticFeature] {
			continue
		}
		candidate.Score += semanticWeight * (best - candidate.Features[semanticFeature])
		candidate.Features[semanticFeature] = best
		candidate.Reasons = append(candidate.Reasons, "chosen_topic")
	}
}
