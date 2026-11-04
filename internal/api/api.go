package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"yap-algorithm/internal/cursor"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/store"
)

type Authenticator interface {
	User(context.Context, string) (string, error)
}
type DevAuth struct{}

func (DevAuth) User(_ context.Context, token string) (string, error) {
	u := strings.TrimPrefix(token, "dev:")
	if u == token || !domain.UUIDValid(u) {
		return "", domain.ErrForbidden
	}
	return u, nil
}

// Supabase Auth validates both legacy HMAC and asymmetric projects. No unverified
// JWT payload, user_metadata, user ID header, or service key is trusted as identity.
type SupabaseAuth struct {
	URL, Key string
	Client   *http.Client
}

func (a SupabaseAuth) User(ctx context.Context, token string) (string, error) {
	r, e := http.NewRequestWithContext(ctx, "GET", a.URL+"/auth/v1/user", nil)
	if e != nil {
		return "", e
	}
	r.Header.Set("apikey", a.Key)
	r.Header.Set("Authorization", "Bearer "+token)
	resp, e := a.Client.Do(r)
	if e != nil {
		return "", e
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			return "", domain.ErrForbidden
		}
		return "", errors.New("auth_unavailable")
	}
	var u struct {
		ID string `json:"id"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&u) != nil || !domain.UUIDValid(u.ID) {
		return "", domain.ErrForbidden
	}
	return u.ID, nil
}

type bucket struct {
	tokens float64
	at     time.Time
}
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]bucket
}

func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.buckets == nil {
		l.buckets = map[string]bucket{}
	}
	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		b = bucket{120, now}
	}
	rate, burst := 2.0, 120.0
	if strings.HasPrefix(key, "ip:") {
		rate, burst = 60, 240
		if !ok {
			b.tokens = burst
		}
	}
	b.tokens += now.Sub(b.at).Seconds() * rate
	if b.tokens > burst {
		b.tokens = burst
	}
	b.at = now
	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	if len(l.buckets) > 10000 {
		for k, v := range l.buckets {
			if now.Sub(v.at) > 2*time.Minute {
				delete(l.buckets, k)
			}
		}
		if len(l.buckets) > 20000 && !ok {
			return false
		}
	}
	l.buckets[key] = b
	return allowed
}

type API struct {
	Store   *store.Store
	Auth    Authenticator
	Cursor  cursor.Codec
	Scorer  store.Scorer
	limiter Limiter
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, c := context.WithTimeout(r.Context(), time.Second)
		defer c()
		if e := a.Store.Pool.Ping(ctx); e != nil {
			write(w, 503, map[string]string{"status": "unavailable"})
			return
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /v1/feed", a.secure(a.feed))
	mux.HandleFunc("POST /v1/posts", a.secure(a.create))
	mux.HandleFunc("GET /v1/posts/{id}", a.secure(a.get))
	mux.HandleFunc("PATCH /v1/posts/{id}", a.secure(a.edit))
	mux.HandleFunc("DELETE /v1/posts/{id}", a.secure(a.remove))
	mux.HandleFunc("POST /v1/posts/{id}/actions", a.secure(a.action))
	mux.HandleFunc("POST /v1/posts/{id}/comments", a.secure(a.comment))
	mux.HandleFunc("PUT /v1/comments/{id}/like", a.secure(a.commentLike))
	mux.HandleFunc("DELETE /v1/comments/{id}/like", a.secure(a.commentLike))
	mux.HandleFunc("POST /v1/posts/{id}/reports", a.secure(a.report))
	mux.HandleFunc("POST /v1/comments/{id}/reports", a.secure(a.report))
	mux.HandleFunc("POST /v1/events:batch", a.secure(a.events))
	mux.HandleFunc("PUT /v1/follows/{id}", a.secure(a.follow))
	mux.HandleFunc("DELETE /v1/follows/{id}", a.secure(a.follow))
	mux.HandleFunc("PUT /v1/preferences/hidden-authors/{id}", a.secure(a.hide))
	mux.HandleFunc("DELETE /v1/preferences/hidden-authors/{id}", a.secure(a.hide))
	mux.HandleFunc("POST /v1/preferences/reset", a.secure(a.reset))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := domain.UUID()
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		start := time.Now()
		defer func() {
			if p := recover(); p != nil {
				slog.Error("request_panic", "request_id", id)
				write(w, 500, map[string]string{"error": "internal_error"})
			}
			slog.Info("request", "request_id", id, "method", r.Method, "route", r.Pattern, "duration_ms", time.Since(start).Milliseconds())
		}()
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		mux.ServeHTTP(w, r)
	})
}

type endpoint func(http.ResponseWriter, *http.Request, string) error

func (a *API) secure(fn endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if !a.limiter.Allow("ip:" + host) {
			write(w, 429, map[string]string{"error": "rate_limited"})
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || len(auth) > 8192 {
			write(w, 401, map[string]string{"error": "authentication_required"})
			return
		}
		ctx, cancel := store.Deadline(r.Context())
		defer cancel()
		r = r.WithContext(ctx)
		user, e := a.Auth.User(ctx, strings.TrimPrefix(auth, "Bearer "))
		if e != nil {
			if errors.Is(e, domain.ErrForbidden) {
				write(w, 401, map[string]string{"error": "invalid_session"})
			} else {
				w.Header().Set("Retry-After", "1")
				write(w, 503, map[string]string{"error": "auth_unavailable"})
			}
			return
		}
		if !a.limiter.Allow("user:" + user) {
			write(w, 429, map[string]string{"error": "rate_limited"})
			return
		}
		if id := r.PathValue("id"); id != "" && !domain.UUIDValid(id) {
			failure(w, domain.ErrInvalid)
			return
		}
		if e = fn(w, r, user); e != nil {
			failure(w, e)
		}
	}
}
func decode(r *http.Request, v any) error {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return domain.ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return domain.ErrInvalid
	}
	return nil
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func failure(w http.ResponseWriter, e error) {
	code, status := "internal_error", 500
	switch {
	case errors.Is(e, domain.ErrInvalid):
		code, status = "invalid_request", 400
	case errors.Is(e, domain.ErrForbidden):
		code, status = "access_denied", 403
	case errors.Is(e, domain.ErrNotFound):
		code, status = "not_found", 404
	case errors.Is(e, domain.ErrExpired):
		code, status = "session_expired", 410
	case errors.Is(e, domain.ErrInvalidated):
		code, status = "session_invalidated", 409
	case errors.Is(e, context.DeadlineExceeded):
		code, status = "request_timeout", 504
	default:
		var p *pgconn.PgError
		if errors.As(e, &p) {
			switch p.Code {
			case "42501":
				code, status = "access_denied", 403
			case "23514", "23503", "22P02":
				code, status = "invalid_request", 400
			case "23505":
				code, status = "conflict", 409
			case "57014", "55P03":
				code, status = "request_timeout", 503
			}
		}
	}
	if status >= 500 {
		slog.Error("request_failed", "error", e)
	}
	write(w, status, map[string]string{"error": code})
}
func (a *API) feed(w http.ResponseWriter, r *http.Request, user string) error {
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "for_you"
	}
	if tab != "for_you" && tab != "friends" && tab != "nearby" {
		return domain.ErrInvalid
	}
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		var e error
		limit, e = strconv.Atoi(raw)
		if e != nil || limit < 1 || limit > 30 {
			return domain.ErrInvalid
		}
	}
	offset := 0
	id := ""
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		v, e := a.Cursor.Decode(raw, user, tab, limit)
		if e != nil {
			return e
		}
		id, offset = v.SessionID, v.Offset
	} else {
		var e error
		id, e = a.Store.Build(r.Context(), user, tab, a.Scorer)
		if e != nil {
			return e
		}
	}
	page, next, e := a.Store.Page(r.Context(), user, tab, id, offset, limit)
	if e != nil {
		return e
	}
	if next >= 0 {
		page.NextCursor = a.Cursor.Encode(cursor.Value{SessionID: id, UserID: user, Tab: tab, Offset: next, Limit: limit})
	}
	write(w, 200, page)
	return nil
}
func (a *API) create(w http.ResponseWriter, r *http.Request, u string) error {
	var p domain.NewPost
	if e := decode(r, &p); e != nil {
		return e
	}
	if p.Photos == nil {
		p.Photos = []string{}
	}
	post, e := a.Store.CreatePost(r.Context(), u, p)
	if e != nil {
		return e
	}
	write(w, 201, post)
	return nil
}
func (a *API) get(w http.ResponseWriter, r *http.Request, u string) error {
	p, e := a.Store.GetPost(r.Context(), u, r.PathValue("id"))
	if e != nil {
		return e
	}
	write(w, 200, p)
	return nil
}
func (a *API) edit(w http.ResponseWriter, r *http.Request, u string) error {
	var in struct {
		Text    string `json:"text"`
		Version int    `json:"version"`
	}
	if e := decode(r, &in); e != nil {
		return e
	}
	if e := a.Store.EditPost(r.Context(), u, r.PathValue("id"), in.Text, in.Version); e != nil {
		return e
	}
	write(w, 200, map[string]string{"status": "updated"})
	return nil
}
func (a *API) remove(w http.ResponseWriter, r *http.Request, u string) error {
	if e := a.Store.DeletePost(r.Context(), u, r.PathValue("id")); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (a *API) action(w http.ResponseWriter, r *http.Request, u string) error {
	var in struct {
		Kind      string `json:"kind"`
		SessionID string `json:"session_id"`
		Position  int    `json:"position"`
	}
	if e := decode(r, &in); e != nil {
		return e
	}
	if in.SessionID != "" && !domain.UUIDValid(in.SessionID) {
		return domain.ErrInvalid
	}
	if e := a.Store.Action(r.Context(), u, r.PathValue("id"), in.Kind, in.SessionID, in.Position); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (a *API) events(w http.ResponseWriter, r *http.Request, u string) error {
	var in struct {
		Events []domain.Event `json:"events"`
	}
	if e := decode(r, &in); e != nil {
		return e
	}
	if e := a.Store.Events(r.Context(), u, in.Events); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (a *API) follow(w http.ResponseWriter, r *http.Request, u string) error {
	if e := a.Store.Follow(r.Context(), u, r.PathValue("id"), r.Method == "PUT"); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (a *API) hide(w http.ResponseWriter, r *http.Request, u string) error {
	if e := a.Store.HideAuthor(r.Context(), u, r.PathValue("id"), r.Method == "PUT"); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (a *API) reset(w http.ResponseWriter, r *http.Request, u string) error {
	if e := a.Store.Reset(r.Context(), u); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (a *API) comment(w http.ResponseWriter, r *http.Request, u string) error {
	var in store.Comment
	if e := decode(r, &in); e != nil {
		return e
	}
	out, e := a.Store.Comment(r.Context(), u, r.PathValue("id"), in)
	if e != nil {
		return e
	}
	write(w, 201, out)
	return nil
}
func (a *API) commentLike(w http.ResponseWriter, r *http.Request, u string) error {
	if e := a.Store.CommentLike(r.Context(), u, r.PathValue("id"), r.Method == "PUT"); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
func (a *API) report(w http.ResponseWriter, r *http.Request, u string) error {
	var in struct {
		Reason string `json:"reason"`
	}
	if e := decode(r, &in); e != nil {
		return e
	}
	if e := a.Store.Report(r.Context(), u, r.PathValue("id"), in.Reason, strings.HasPrefix(r.URL.Path, "/v1/comments/")); e != nil {
		return e
	}
	w.WriteHeader(204)
	return nil
}
