package ranking

import (
	"math"
	"testing"
	"time"
)

func TestProfileSeparatesTopicsAndSigns(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	evidence := []Evidence{{PostID: "a", Vector: vec(0), Weight: 2, At: now}, {PostID: "b", Vector: vec(0), Weight: 2, At: now},
		{PostID: "c", Vector: vec(1), Weight: 2, At: now}, {PostID: "d", Vector: vec(0), Weight: -3, At: now}}
	profile := BuildProfile(evidence, now)
	if len(profile) != 3 {
		t.Fatalf("topics/signs merged incorrectly: %d", len(profile))
	}
	var positive, negative bool
	for _, c := range profile {
		if CosineForTest(c.Vector, vec(0)) > .99 {
			if c.Negative {
				negative = true
			} else {
				positive = true
				if c.Weight != 4 {
					t.Fatal("bounded evidence not aggregated")
				}
			}
		}
	}
	if !positive || !negative {
		t.Fatal("positive and negative topic must stay independent")
	}
	if evidence[0].Vector[0] != 1 {
		t.Fatal("input mutated")
	}
}

func CosineForTest(a, b []float64) float64 {
	var sum float64
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func TestProfileBudgetsAndDecay(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	var evidence []Evidence
	for i := 0; i < 20; i++ {
		weight := 2.0
		if i >= 10 {
			weight = -3
		}
		evidence = append(evidence, Evidence{PostID: string(rune('a' + i)), Vector: vec(i), Weight: weight, At: now})
	}
	evidence = append(evidence, Evidence{PostID: "invalid", Vector: vec(0), Weight: math.NaN(), At: now}, Evidence{PostID: "old", Vector: vec(0), Weight: 5, At: now.Add(-31 * 24 * time.Hour)})
	profile := BuildProfile(evidence, now)
	positive, negative := 0, 0
	for _, c := range profile {
		if c.Negative {
			negative++
		} else {
			positive++
		}
	}
	if positive != 4 || negative != 4 {
		t.Fatal("profile exceeded or lost topic budget")
	}
	decayed := BuildProfile([]Evidence{{PostID: "a", Vector: vec(0), Weight: 2, At: now.Add(-7 * 24 * time.Hour)}}, now)
	if math.Abs(decayed[0].Weight-2/math.E) > 1e-9 {
		t.Fatal("evidence decay incorrect")
	}
}

func TestForYouSeenPolicyAndDuplicateSources(t *testing.T) {
	var candidates []Candidate
	for i := 0; i < 20; i++ {
		candidates = append(candidates, Candidate{ID: string(rune('a' + i)), AuthorID: "author", Score: 1})
	}
	candidates = append(candidates, Candidate{ID: "seen", AuthorID: "other", Score: 100, Seen: true}, candidates[0])
	selected := SelectForYou(candidates)
	if len(selected) != 20 {
		t.Fatalf("expected 20 unseen posts, got %d", len(selected))
	}
	for _, c := range selected {
		if c.Seen {
			t.Fatal("read post shown despite sufficient unseen inventory")
		}
	}
	small := SelectForYou([]Candidate{{ID: "read", AuthorID: "a", Score: 100, Seen: true}, {ID: "new", AuthorID: "b", Score: 0}})
	if len(small) != 2 || small[0].ID != "new" {
		t.Fatal("small-inventory fallback displaced an unseen post")
	}
}
