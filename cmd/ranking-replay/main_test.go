package main

import (
	"testing"
	"time"

	"yap-algorithm/internal/ranking"
)

func TestRankUsesSameCandidatesForVisionComparison(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := request{
		At: at,
		Candidates: []ranking.Candidate{
			{ID: "photo", AuthorID: "a", CreatedAt: at.Add(-48 * time.Hour), Visual: 1},
			{ID: "fresh", AuthorID: "b", CreatedAt: at.Add(-time.Second)},
		},
		Pools: map[string]pool{
			"hybrid": {IDs: []int{0, 1}},
			"vision": {IDs: []int{0, 1}, Vision: []int{0}},
		},
	}
	got, err := rank(in)
	if err != nil {
		t.Fatal(err)
	}
	if got["hybrid"].Order[0] != "fresh" || got["vision"].Order[0] != "photo" ||
		len(got["vision"].Features) != len(got["hybrid"].Features) {
		t.Fatalf("unexpected comparison: %#v", got)
	}
	in.Evidence = []ranking.Evidence{{PostID: "future", At: at.Add(time.Second)}}
	if _, err := rank(in); err == nil {
		t.Fatal("accepted evidence after the snapshot")
	}
}
