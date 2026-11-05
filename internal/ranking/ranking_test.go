package ranking

import (
	"testing"
	"time"
	"yap-algorithm/internal/domain"
)

func TestDiscoveryUsesAuthorCoverageInsteadOfResettingForANewPost(t *testing.T) {
	established := Candidate{HasAuthorExposure: true, Impressions: 0, AuthorImpressions: 10000, AuthorViewers: 1000}
	newAuthor := Candidate{HasAuthorExposure: true, Impressions: 100, AuthorImpressions: 0, AuthorViewers: 0}
	if underexposed(established) || !underexposed(newAuthor) {
		t.Fatal("a new post reset established-author reach or own views hid a new author")
	}
	now := time.Now()
	if Features(established, nil, now)[5] >= Features(newAuthor, nil, now)[5] {
		t.Fatal("coverage feature does not reflect author reach")
	}
}

func vec(axis int) []float64 { v := make([]float64, domain.Dimensions); v[axis] = 1; return v }
func TestPersonalizationAndNegativeFeedback(t *testing.T) {
	now := time.Now()
	cs := []Candidate{{ID: "a", AuthorID: "one", CreatedAt: now, Embedding: vec(0)}, {ID: "b", AuthorID: "two", CreatedAt: now, Embedding: vec(1)}}
	Score(cs, []Interest{{Vector: vec(0), Weight: 5, UpdatedAt: now}}, now)
	if cs[0].Score <= cs[1].Score {
		t.Fatal("positive interest does not change ordering")
	}
	Score(cs, []Interest{{Vector: vec(0), Weight: 5, UpdatedAt: now, Negative: true}}, now)
	if cs[0].Score >= cs[1].Score {
		t.Fatal("semantic negative feedback does not reduce score")
	}
}
func TestDiversityAndNoDuplicates(t *testing.T) {
	in := []Candidate{{ID: "a", AuthorID: "one", Score: 1, Embedding: vec(0)}, {ID: "b", AuthorID: "one", Score: .99, Embedding: vec(0)}, {ID: "c", AuthorID: "two", Score: .95, Embedding: vec(1)}}
	out := Rerank(in, 10, 0)
	if len(out) != 3 || out[1].ID != "c" {
		t.Fatalf("no author diversity: %+v", out)
	}
	seen := map[string]bool{}
	for _, c := range out {
		if seen[c.ID] {
			t.Fatal("duplicate")
		}
		seen[c.ID] = true
	}
}
