// Command ranking-replay runs the serving ranker against offline candidates.
// It takes JSONL on stdin and has no database or network access.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"yap-algorithm/internal/ranking"
)

type request struct {
	At         time.Time
	Candidates []ranking.Candidate
	Evidence   []ranking.Evidence
	Topics     [][]float64
	Pools      map[string]pool
}

type pool struct {
	IDs    []int
	Vision []int
}

type result struct {
	Order    []string             `json:"order"`
	Features map[string][]float64 `json:"features"`
	Millis   float64              `json:"ms"`
}

func rank(input request) (map[string]result, error) {
	if input.At.IsZero() || len(input.Candidates) > 1200 || len(input.Evidence) > 128 || len(input.Topics) > 4 || len(input.Pools) > 16 {
		return nil, errors.New("offline ranking request exceeds limits")
	}
	for _, item := range input.Candidates {
		if item.ID == "" || item.AuthorID == "" || item.CreatedAt.After(input.At) {
			return nil, errors.New("invalid offline candidate")
		}
	}
	for _, item := range input.Evidence {
		if !item.At.Before(input.At) {
			return nil, errors.New("offline evidence must predate the snapshot")
		}
	}
	profile := ranking.BuildProfile(input.Evidence, input.At)
	confidence := ranking.TopicConfidence(input.Evidence, input.At)
	output := make(map[string]result, len(input.Pools))
	for name, selection := range input.Pools {
		if len(selection.IDs) > 1200 || len(selection.Vision) > 32 {
			return nil, errors.New("offline pool exceeds limits")
		}
		start := time.Now()
		visual := make(map[int]bool, len(selection.Vision))
		for _, i := range selection.Vision {
			visual[i] = true
		}
		candidates := make([]ranking.Candidate, 0, len(selection.IDs))
		features := make(map[string][]float64, len(selection.IDs))
		for _, i := range selection.IDs {
			if i < 0 || i >= len(input.Candidates) {
				return nil, errors.New("offline pool references an unknown candidate")
			}
			item := input.Candidates[i]
			if !visual[i] {
				item.Visual = 0
			}
			candidates = append(candidates, item)
		}
		ranking.Score(candidates, profile, input.At)
		ranking.ApplyTopicPrior(candidates, input.Topics, confidence)
		for _, item := range candidates {
			features[item.ID] = item.Features
		}
		order := make([]string, 0, len(candidates))
		for _, item := range ranking.SelectForYou(candidates) {
			order = append(order, item.ID)
		}
		output[name] = result{Order: order, Features: features, Millis: float64(time.Since(start)) / float64(time.Millisecond)}
	}
	return output, nil
}

func run(in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	encoder := json.NewEncoder(out)
	for line := 1; scanner.Scan(); line++ {
		var input request
		if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		output, err := rank(input)
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if err := encoder.Encode(output); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func main() {
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
