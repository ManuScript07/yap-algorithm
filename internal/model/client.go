package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"
	"yap-algorithm/internal/domain"
)

type Client struct {
	URL, Revision string
	HTTP          *http.Client
}

func New(url, revision string) *Client {
	return &Client{url, revision, &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}
func (c *Client) call(ctx context.Context, path string, input, output any) error {
	b, e := json.Marshal(input)
	if e != nil {
		return e
	}
	r, e := http.NewRequestWithContext(ctx, "POST", c.URL+path, bytes.NewReader(b))
	if e != nil {
		return e
	}
	r.Header.Set("Content-Type", "application/json")
	resp, e := c.HTTP.Do(r)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("model_status_%d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(output)
}
func (c *Client) Embed(ctx context.Context, text string) (string, error) {
	vector, err := c.Encode(ctx, text)
	if err != nil {
		return "", err
	}
	return domain.Vector(vector)
}

func (c *Client) Encode(ctx context.Context, text string) ([]float64, error) {
	var out struct {
		Embedding []float64 `json:"embedding"`
		Revision  string    `json:"revision"`
	}
	if e := c.call(ctx, "/embed", map[string]string{"text": text}, &out); e != nil {
		return nil, e
	}
	if out.Revision != c.Revision {
		return nil, errors.New("embedding_revision_mismatch")
	}
	return domain.NormalizeVector(out.Embedding)
}

func (c *Client) EncodeBatch(ctx context.Context, texts []string) ([][]float64, error) {
	if len(texts) < 1 || len(texts) > 8 {
		return nil, domain.ErrInvalid
	}
	var out struct {
		Embeddings [][]float64 `json:"embeddings"`
		Revision   string      `json:"revision"`
	}
	if err := c.call(ctx, "/embed-batch", map[string]any{"texts": texts}, &out); err != nil {
		return nil, err
	}
	if out.Revision != c.Revision || len(out.Embeddings) != len(texts) {
		return nil, errors.New("embedding_batch_contract_mismatch")
	}
	for i := range out.Embeddings {
		vector, err := domain.NormalizeVector(out.Embeddings[i])
		if err != nil {
			return nil, err
		}
		out.Embeddings[i] = vector
	}
	return out.Embeddings, nil
}
func (c *Client) Score(ctx context.Context, features [][]float64) ([]float64, string, error) {
	var out struct {
		Scores  []float64 `json:"scores"`
		Version string    `json:"version"`
	}
	if e := c.call(ctx, "/score", map[string]any{"features": features, "feature_version": "hybrid-v1"}, &out); e != nil {
		return nil, "", e
	}
	if len(out.Scores) != len(features) || out.Version == "" {
		return nil, "", domain.ErrInvalid
	}
	for _, s := range out.Scores {
		if math.IsInf(s, 0) || math.IsNaN(s) {
			return nil, "", domain.ErrInvalid
		}
	}
	return out.Scores, out.Version, nil
}
