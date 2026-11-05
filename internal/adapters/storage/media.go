package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"yap-algorithm/internal/application"
)

type Media struct {
	base, key, token string
	client           *http.Client
}

func NewMedia(endpoint, key, token string) (*Media, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("invalid SUPABASE_URL")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || ip != nil && ip.IsLoopback())) {
		return nil, errors.New("remote Storage requires HTTPS")
	}
	// Refuse accidental service_role/API-secret credentials. The separate Storage
	// token must select the limited media role; this is configuration validation,
	// not signature verification (Supabase verifies the signed JWT on each call).
	parts := strings.Split(token, ".")
	if len(parts) != 3 || key == "" {
		return nil, errors.New("a limited Storage JWT and API key are required")
	}
	if keyParts := strings.Split(key, "."); len(keyParts) == 3 {
		keyClaims, err := decodeClaims(keyParts[1])
		if err != nil || keyClaims.Role != "anon" {
			return nil, errors.New("Storage API key must be publishable/anon")
		}
	} else if !strings.HasPrefix(key, "sb_publishable_") {
		return nil, errors.New("Storage API key must be publishable/anon")
	}
	claims, err := decodeClaims(parts[1])
	if err != nil || claims.Role != "yap_feed_media_worker" || claims.Expires <= time.Now().Unix()+60 {
		return nil, errors.New("Storage token must be unexpired and restricted to yap_feed_media_worker")
	}
	return &Media{base: strings.TrimRight(endpoint, "/") + "/storage/v1", key: key, token: token,
		client: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}}, nil
}

func objectPath(bucket, path string) (string, error) {
	if bucket != "feed-uploads" && bucket != "feed-media" || path == "" || len(path) > 300 {
		return "", errors.New("invalid media object path")
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", errors.New("invalid media object path")
		}
		for _, c := range segment {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
				return "", errors.New("invalid media object path")
			}
		}
	}
	return bucket + "/" + path, nil
}

func (s *Media) request(ctx context.Context, method, path string, data []byte, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("cannot create Storage request")
	}
	req.Header.Set("apikey", s.key)
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Cache-Control", "private, no-store")
	req.Header.Set("x-upsert", "false")
	response, err := s.client.Do(req)
	if err != nil {
		return nil, errors.New("Storage request failed")
	}
	return response, nil
}

func (s *Media) Download(ctx context.Context, path string) ([]byte, error) {
	object, err := objectPath("feed-uploads", path)
	if err != nil {
		return nil, err
	}
	response, err := s.request(ctx, "GET", "/object/authenticated/"+object, nil, "")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, errors.New("Storage download rejected")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, application.MaxPhotoBytes+1))
	if err != nil {
		return nil, errors.New("Storage body read failed")
	}
	if len(data) > application.MaxPhotoBytes {
		return nil, application.ErrInvalidPhoto
	}
	return data, nil
}

func (s *Media) Upload(ctx context.Context, path string, data []byte) error {
	object, err := objectPath("feed-media", path)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > application.MaxPhotoBytes {
		return application.ErrInvalidPhoto
	}
	response, err := s.request(ctx, "POST", "/object/"+object, data, "image/jpeg")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("Storage upload rejected")
	}
	return nil
}

func (s *Media) Delete(ctx context.Context, bucket, path string) error {
	if _, err := objectPath(bucket, path); err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{"prefixes": []string{path}})
	response, err := s.request(ctx, "DELETE", "/object/"+bucket, body, "application/json")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	// An empty response is normal after a retry of an already removed object.
	// The repository MUST independently check Storage metadata before ack: RLS
	// can also make an existing object invisible to the removal request.
	var rows []struct {
		Name string `json:"name"`
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if err := json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&rows); err == nil {
			if len(rows) == 0 {
				return nil
			}
			for _, row := range rows {
				if row.Name == path {
					return nil
				}
			}
		}
	}
	return errors.New("Storage removal not confirmed")
}
