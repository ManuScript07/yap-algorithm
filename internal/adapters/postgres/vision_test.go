package postgres

import (
	"fmt"
	"testing"

	"yap-algorithm/internal/ranking"
)

func TestMergeVisionPreservesCandidateBudgetAndVersion(t *testing.T) {
	base := make([]ranking.Candidate, 1200)
	for i := range base {
		base[i] = ranking.Candidate{ID: fmt.Sprintf("base-%d", i), PostVersion: 2, Sources: []string{"fresh"}}
	}
	base[1].Embedding = []float64{1}
	visual := []ranking.Candidate{
		{ID: "base-0", PostVersion: 1, Visual: 0.9},
		{ID: "base-1", PostVersion: 2, Visual: 0.9},
		{ID: "base-2", PostVersion: 2, Visual: 0.7},
		{ID: "new", PostVersion: 1, Visual: 0.8},
	}
	merged := mergeVision(base, visual)
	if len(merged) != 1200 || merged[0].Visual != 0 || merged[1].Visual != 0 ||
		merged[2].Visual != 0.7 || len(merged[2].Sources) != 2 || merged[1199].ID != "new" {
		t.Fatalf("bad vision merge: %v %v %v, tail=%v, count=%d", merged[0], merged[1], merged[2], merged[1199], len(merged))
	}
}
