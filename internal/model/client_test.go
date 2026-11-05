package model

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"yap-algorithm/internal/domain"
)

func TestBatchEncoderRejectsMalformedResponses(t *testing.T) {
	for _, kind := range []string{"ok", "wrong-revision", "short", "invalid-vector", "unavailable"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/embed-batch" {
					t.Error("unexpected model route", r.URL.Path)
				}
				if kind == "unavailable" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				var input struct{ Texts []string }
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil || len(input.Texts) != 2 || input.Texts[1] != "second" {
					t.Error("request order/shape changed", err)
				}
				vectors := make([][]float64, 2)
				for i := range vectors {
					vectors[i] = make([]float64, domain.Dimensions)
					vectors[i][i] = 1
				}
				revision := "test-space"
				switch kind {
				case "wrong-revision":
					revision = "another-space"
				case "short":
					vectors = vectors[:1]
				case "invalid-vector":
					vectors[1] = []float64{1}
				}
				json.NewEncoder(w).Encode(map[string]any{"embeddings": vectors, "revision": revision})
			}))
			defer server.Close()
			vectors, err := New(server.URL, "test-space").EncodeBatch(context.Background(), []string{"first", "second"})
			if kind == "ok" {
				if err != nil || vectors[0][0] != 1 || vectors[1][1] != 1 {
					t.Fatal("valid mapping lost", err)
				}
			} else if err == nil || vectors != nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
}
