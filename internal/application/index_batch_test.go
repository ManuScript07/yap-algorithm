package application

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"yap-algorithm/internal/domain"
)

type batchMemory struct {
	jobs    []IndexJob
	results []IndexCompletion
	retries []int64
}

func (m *batchMemory) ClaimBatch(context.Context, string, int) ([]IndexJob, error) {
	return m.jobs, nil
}
func (m *batchMemory) CompleteBatch(_ context.Context, results []IndexCompletion) error {
	m.results = results
	return nil
}
func (m *batchMemory) Retry(_ context.Context, job IndexJob) error {
	m.retries = append(m.retries, job.ID)
	return nil
}

type batchEncodeFunc func(context.Context, []string) ([][]float64, error)

func (f batchEncodeFunc) EncodeBatch(ctx context.Context, texts []string) ([][]float64, error) {
	return f(ctx, texts)
}

func TestBatchIndexerPreservesOrderAndFences(t *testing.T) {
	memory := &batchMemory{jobs: []IndexJob{
		{ID: 1, Text: "first", Lease: "lease-1", Version: 2},
		{ID: 2, Text: "", Lease: "superseded", Version: 1},
		{ID: 3, Text: "second", Lease: "lease-3", Version: 4},
	}}
	encoder := batchEncodeFunc(func(_ context.Context, texts []string) ([][]float64, error) {
		if !reflect.DeepEqual(texts, []string{"first", "second"}) {
			t.Fatalf("model input changed source order: %v", texts)
		}
		vectors := make([][]float64, 2)
		for i := range vectors {
			vectors[i] = make([]float64, domain.Dimensions)
			vectors[i][i] = 2
		}
		return vectors, nil
	})
	worked, err := (BatchIndexer{Repository: memory, Encoder: encoder, Kind: "live", Size: 3}).Once(context.Background())
	if err != nil || !worked || len(memory.results) != 3 {
		t.Fatalf("batch completion: %v", err)
	}
	if memory.results[0].Vector[0] != 1 || memory.results[1].Vector != nil || memory.results[2].Vector[1] != 1 || memory.results[2].Job.Lease != "lease-3" || memory.results[2].Job.Version != 4 {
		t.Fatal("model rows, invalidation placeholder or source fences changed")
	}
}

func TestBatchIndexerFailureDoesNotPartiallyPublish(t *testing.T) {
	for _, kind := range []string{"outage", "short", "invalid", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			memory := &batchMemory{jobs: []IndexJob{{ID: 1, Text: "a"}, {ID: 2, Text: "b"}}}
			encoder := batchEncodeFunc(func(context.Context, []string) ([][]float64, error) {
				switch kind {
				case "outage":
					return nil, errors.New("model unavailable")
				case "short":
					return [][]float64{}, nil
				case "cancel":
					cancel()
					return nil, context.Canceled
				default:
					return [][]float64{{1}, {1}}, nil
				}
			})
			worked, err := (BatchIndexer{Repository: memory, Encoder: encoder, Kind: "backfill", Size: 2}).Once(ctx)
			wantRetries := 2
			if kind == "cancel" {
				wantRetries = 0
			}
			if !worked || err == nil || memory.results != nil || len(memory.retries) != wantRetries {
				t.Fatalf("failure published or lost a retry: err=%v retries=%v", err, memory.retries)
			}
		})
	}
}

func TestPreparationLanesMakeProgressUnderContinuousLoad(t *testing.T) {
	counts := [4]int{}
	lane := func(i int) Processor {
		return processorFunc(func(context.Context) (bool, error) { counts[i]++; return true, nil })
	}
	pipeline := Pipeline{Processors: []Processor{lane(0), lane(1), lane(0), lane(2), lane(3)}}
	for i := 0; i < 100; i++ {
		if _, err := pipeline.Once(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if counts != [4]int{40, 20, 20, 20} {
		t.Fatalf("interactive/live/scheduled/backfill service shares: %v", counts)
	}
}
