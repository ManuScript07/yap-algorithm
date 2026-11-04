package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"yap-algorithm/internal/domain"
)

type brokenAuth struct{ err error }

func (b brokenAuth) User(context.Context, string) (string, error) { return "", b.err }
func TestAuthOutageDoesNotInvalidateSession(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{{domain.ErrForbidden, 401}, {errors.New("connection_unavailable"), 503}} {
		a := API{Auth: brokenAuth{test.err}}
		r := httptest.NewRequest("GET", "/v1/feed", nil)
		r.Header.Set("Authorization", "Bearer a-token")
		w := httptest.NewRecorder()
		a.Handler().ServeHTTP(w, r)
		if w.Code != test.status {
			t.Fatalf("got %d want %d", w.Code, test.status)
		}
	}
}
