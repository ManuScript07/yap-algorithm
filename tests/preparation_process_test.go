package tests

import (
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Run the shipped binary with only its restricted local identity. Tests own the
// process and terminate it before fixture cleanup, including assertion failures.
func startPreparationWorker(t *testing.T, modelURL, logPath string) {
	t.Helper()
	startPreparationWorkerProcess(t, localRecommendationDSN(t), modelURL, logPath)
}

func startPreparationWorkerProcess(t *testing.T, dsn, modelURL, logPath string) func() {
	t.Helper()
	endpoint, err := url.Parse(modelURL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Hostname() != "127.0.0.1" {
		t.Fatal("local model required")
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
	worker.Env = append(worker.Env, "DATABASE_URL="+dsn, "MODEL_URL="+modelURL)
	worker.Stdout, worker.Stderr = logFile, logFile
	if err := worker.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- worker.Wait() }()
	stop := sync.OnceFunc(func() { _ = worker.Process.Kill(); <-stopped; _ = logFile.Close() })
	t.Cleanup(stop)
	return stop
}
