package ranking

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestTopicPriorDiminishesOnlyWithDistinctRecentPositivePosts(t *testing.T) {
	now := time.Now()
	evidence := []Evidence{
		{PostID: "negative", Weight: -3, At: now},
		{PostID: "old", Weight: 2, At: now.Add(-15 * 24 * time.Hour)},
		{PostID: "passive", Weight: .2, At: now},
		{PostID: "follow-only", Weight: 1, At: now},
	}
	if got := TopicConfidence(evidence, now); got != .70 {
		t.Fatalf("irrelevant events changed cold-start weight: %.3f", got)
	}
	for i := 0; i < 4; i++ {
		item := Evidence{PostID: fmt.Sprint(i), Weight: 2, At: now}
		evidence = append(evidence, item, item)
	}
	if got := TopicConfidence(evidence, now); got != .35 {
		t.Fatalf("four distinct actions should halve the prior: %.3f", got)
	}
	for i := 4; i < 16; i++ {
		evidence = append(evidence, Evidence{PostID: fmt.Sprint(i), Weight: 2, At: now})
	}
	if got := TopicConfidence(evidence, now); got != 0 {
		t.Fatalf("mature profile still spends ANN budget on onboarding: %.3f", got)
	}
}

func TestTopicPriorCapsRetrievalAndDoesNotStackWithLearnedInterests(t *testing.T) {
	now := time.Now()
	picture, opposite := []float64{1, 0}, []float64{0, 1}
	profile := []Interest{{Vector: picture, Weight: 3, UpdatedAt: now},
		{Vector: opposite, Weight: 3, UpdatedAt: now, Negative: true}}
	duplicateTopics := [][]float64{opposite, opposite, opposite, opposite, opposite}
	seeds := TopicRetrieval(profile, duplicateTopics, .70)
	if len(seeds) != 5 || len(profile) != 2 || !profile[1].Negative {
		t.Fatalf("retrieval modified the learned profile: %+v", seeds)
	}
	withHistory := []Candidate{{ID: "learned", Embedding: picture}, {ID: "suppressed", Embedding: opposite}}
	Score(withHistory, profile, now)
	learned, suppressed := withHistory[0].Score, withHistory[1].Score
	ApplyTopicPrior(withHistory, duplicateTopics, .70)
	if withHistory[0].Score != learned || math.Abs(withHistory[1].Score-suppressed-.46*.70) > 1e-9 ||
		withHistory[1].Features[4] < .99 || withHistory[1].Score >= withHistory[0].Score {
		t.Fatal("topic confidence stacked or removed the learned negative signal")
	}
}
