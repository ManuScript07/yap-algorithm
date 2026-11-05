package tests

import (
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Run the shipped binary with only its restricted local identity. Tests own the
// process and terminate it before fixture cleanup, including assertion failures.
func startPreparationWorker(t *testing.T, modelURL, logPath string) {
	t.Helper()
	endpoint, err := url.Parse(modelURL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Hostname() != "127.0.0.1" {
		t.Fatal("local model required")
	}
	stage5Worker(t)
	raw, err := os.ReadFile("../.local/supabase-worker.json")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		DSN string `json:"database_url"`
	}
	if json.Unmarshal(raw, &config) != nil {
		t.Fatal("private worker config")
	}
	binary := filepath.Join(t.TempDir(), "preparation-worker.exe")
	build := exec.Command("go", "build", "-o", binary, "./cmd/worker")
	build.Dir = ".."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("worker build: %v %s", err, output)
	}
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	worker := exec.Command(binary, "-concurrency", "2", "-batch-size", "4")
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "YAP_") || strings.HasPrefix(name, "SUPABASE_") || strings.HasPrefix(name, "MEDIA_") || strings.HasPrefix(name, "PG") || name == "DATABASE_URL" || name == "MODEL_URL" {
			continue
		}
		worker.Env = append(worker.Env, entry)
	}
	worker.Env = append(worker.Env, "DATABASE_URL="+config.DSN, "MODEL_URL="+modelURL)
	worker.Stdout, worker.Stderr = logFile, logFile
	if err := worker.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- worker.Wait() }()
	t.Cleanup(func() { _ = worker.Process.Kill(); <-stopped; _ = logFile.Close() })
}
