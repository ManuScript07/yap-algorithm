package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"yap-algorithm/internal/application"
)

func fixtureToken(role string, expires int64) string {
	body, _ := json.Marshal(map[string]any{"role": role, "exp": expires})
	return "test." + base64.RawURLEncoding.EncodeToString(body) + ".signature"
}

func TestConfigurationAndObjectBoundaries(t *testing.T) {
	token := fixtureToken("yap_feed_media_worker", time.Now().Add(time.Hour).Unix())
	for _, endpoint := range []string{"http://db.example.com", "https://user:password@db.example.com", "https://db.example.com/path", "https://db.example.com?token=test"} {
		if _, err := NewMedia(endpoint, "sb_publishable_test", token); err == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	for _, credential := range []string{fixtureToken("service_role", time.Now().Add(time.Hour).Unix()), fixtureToken("yap_feed_media_worker", 1), "invalid"} {
		if _, err := NewMedia("https://db.example.com", "sb_publishable_test", credential); err == nil {
			t.Fatal("excessive/expired token accepted")
		}
	}
	if _, err := NewMedia("https://db.example.com", fixtureToken("service_role", time.Now().Unix()+3600), token); err == nil {
		t.Fatal("service_role used as API key")
	}
	for _, path := range []string{"../other", "/root", "a//b", "a/%2e%2e/b", "a\\b", "https://db.example.com"} {
		if _, err := objectPath("feed-media", path); err == nil {
			t.Fatal("path escaped its registered object")
		}
	}
}

func TestTransportLimitsRedirectsAndImmutableUploads(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/redirect"):
			http.Redirect(w, r, target.URL, http.StatusFound)
		case strings.HasSuffix(r.URL.Path, "/large"):
			io.Copy(w, io.LimitReader(zeroReader{}, application.MaxPhotoBytes+1))
		case r.Method == "POST":
			if r.Header.Get("x-upsert") != "false" || r.Header.Get("Content-Type") != "image/jpeg" {
				t.Error("output upload was mutable or had wrong MIME")
			}
			w.Write([]byte(`{}`))
		case r.Method == "DELETE":
			w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	objects, err := NewMedia(server.URL, "sb_publishable_test", fixtureToken("yap_feed_media_worker", time.Now().Unix()+3600))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Download(context.Background(), "redirect"); err == nil || redirected.Load() != 0 {
		t.Fatal("Storage credentials followed a redirect")
	}
	if _, err := objects.Download(context.Background(), "large"); err != application.ErrInvalidPhoto {
		t.Fatal("unbounded Storage download")
	}
	if err := objects.Upload(context.Background(), "image.jpg", []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := objects.Delete(context.Background(), "feed-media", "missing.jpg"); err != nil {
		t.Fatal("missing-object retry must be verified by database, not fail transport")
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }
