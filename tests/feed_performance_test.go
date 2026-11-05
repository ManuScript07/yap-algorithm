package tests

import "testing"

func TestFeedTimingNearestRankPercentiles(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(100 - i)
	}
	summary := summarizeFeedTiming(values)
	if summary.Count != 100 || summary.P50 != 50 || summary.P95 != 95 || summary.P99 != 99 || summary.Max != 100 {
		t.Fatalf("incorrect nearest-rank percentiles: %+v", summary)
	}
	if values[0] != 100 {
		t.Fatal("summary mutated original observations")
	}
	if got := summarizeFeedTiming([]float64{7}); got.P50 != 7 || got.P95 != 7 || got.P99 != 7 || got.Max != 7 {
		t.Fatal(got)
	}
	if got := summarizeFeedTiming(nil); got.Count != 0 {
		t.Fatal(got)
	}
}
