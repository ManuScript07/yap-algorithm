package datasetsim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// Local client intentionally has no configurable host, follows no redirects
// and never includes bearer tokens or response bodies in errors.
type Client struct {
	Anon, Service string
	http          *http.Client
}

const localURL = "http://127.0.0.1:54321"

func NewClient(anon, service string) *Client {
	return &Client{Anon: anon, Service: service, http: &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func (c *Client) request(ctx context.Context, method, endpoint, token, mime string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, localURL+endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid local API request")
	}
	req.Header.Set("apikey", c.Anon)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", mime)
	req.Header.Set("x-upsert", "false")
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("local API %s: %w", path.Base(req.URL.Path), err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, (5<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 5<<20 {
		return errors.New("local response exceeds byte budget")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var code struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(raw, &code)
		return fmt.Errorf("local API %s: HTTP %d %s", path.Base(req.URL.Path), res.StatusCode, code.Code)
	}
	if data, ok := out.(*[]byte); ok {
		*data = raw
		return nil
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func (c *Client) JSON(ctx context.Context, method, endpoint, token string, input, output any) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return c.request(ctx, method, endpoint, token, "application/json", raw, output)
}

func (c *Client) RPC(ctx context.Context, name, token string, input, output any) error {
	return c.JSON(ctx, "POST", "/rest/v1/rpc/"+name, token, input, output)
}

func (c *Client) Upload(ctx context.Context, key, mime, owner, token string, raw []byte) error {
	// Generated identifiers only; refuse a path outside this fixture user.
	if !ownedKey(key, owner) {
		return errors.New("upload key is outside synthetic user")
	}
	return c.request(ctx, "POST", "/storage/v1/object/feed-uploads/"+key, token, mime, raw, nil)
}

func ownedKey(key, owner string) bool {
	return strings.HasPrefix(key, owner+"/") && !strings.HasSuffix(key, "/") && path.Clean(key) == key &&
		!strings.ContainsAny(key, "%?#\\")
}

func (c *Client) PreparedPhoto(ctx context.Context, key, owner, token string) ([]byte, error) {
	if !ownedKey(key, owner) {
		return nil, errors.New("prepared photo key is outside synthetic user")
	}
	var raw []byte
	err := c.request(ctx, "GET", "/storage/v1/object/authenticated/feed-media/"+key, token, "", nil, &raw)
	return raw, err
}

func (c *Client) DeleteObjects(ctx context.Context, bucket string, keys []string) error {
	return c.JSON(ctx, "DELETE", "/storage/v1/object/"+url.PathEscape(bucket), c.Service,
		map[string]any{"prefixes": keys}, nil)
}
