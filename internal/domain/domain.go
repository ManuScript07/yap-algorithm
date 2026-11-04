package domain

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

const Dimensions = 384
const MaxPostTextChars = 1000

var ErrInvalid = errors.New("invalid_request")
var ErrForbidden = errors.New("access_denied")
var ErrNotFound = errors.New("not_found")
var ErrExpired = errors.New("session_expired")
var ErrInvalidated = errors.New("session_invalidated")

type Post struct {
	ID        string    `json:"id"`
	AuthorID  string    `json:"author_id"`
	Text      string    `json:"text"`
	Audience  string    `json:"audience"`
	Language  string    `json:"language"`
	Photos    []string  `json:"photos"`
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Location  *Location `json:"precise_location,omitempty"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func (l Location) Valid() bool {
	return !math.IsNaN(l.Latitude) && !math.IsNaN(l.Longitude) && !math.IsInf(l.Latitude, 0) && !math.IsInf(l.Longitude, 0) && math.Abs(l.Latitude) <= 90 && math.Abs(l.Longitude) <= 180
}

type NewPost struct {
	ID       string    `json:"id,omitempty"`
	Text     string    `json:"text"`
	Audience string    `json:"audience"`
	Language string    `json:"language"`
	Photos   []string  `json:"photos"`
	Location *Location `json:"location,omitempty"`
}

func (p NewPost) Validate() error {
	if p.ID != "" && !UUIDValid(p.ID) {
		return ErrInvalid
	}
	if len([]rune(p.Text)) > MaxPostTextChars || (strings.TrimSpace(p.Text) == "" && len(p.Photos) == 0) || len(p.Photos) > 3 {
		return ErrInvalid
	}
	if p.Audience != "public" && p.Audience != "friends" {
		return ErrInvalid
	}
	if p.Language != "ru" && p.Language != "en" {
		return ErrInvalid
	}
	if p.Location != nil && !p.Location.Valid() {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, p := range p.Photos {
		if p == "" || len(p) > 500 || strings.Contains(p, "..") || strings.Contains(p, "://") || seen[p] {
			return ErrInvalid
		}
		seen[p] = true
	}
	return nil
}

type Event struct {
	ID              string  `json:"id"`
	SessionID       string  `json:"session_id"`
	Position        int     `json:"position"`
	PostID          string  `json:"post_id"`
	Kind            string  `json:"kind"`
	VisibleMS       int     `json:"visible_ms"`
	VisibleFraction float64 `json:"visible_fraction"`
	ActiveMS        int     `json:"active_ms"`
}

func (e Event) Validate() error {
	if !UUIDValid(e.ID) || !UUIDValid(e.SessionID) || !UUIDValid(e.PostID) || e.Position < 0 || e.Position > 2000 {
		return ErrInvalid
	}
	if e.VisibleMS < 0 || e.VisibleMS > 300000 || e.ActiveMS < 0 || e.ActiveMS > 120000 || math.IsNaN(e.VisibleFraction) || e.VisibleFraction < 0 || e.VisibleFraction > 1 {
		return ErrInvalid
	}
	switch e.Kind {
	case "impression":
		if e.VisibleMS < 1000 || e.VisibleFraction < 0.5 {
			return ErrInvalid
		}
	case "dwell":
		if e.ActiveMS < 1000 {
			return ErrInvalid
		}
	case "open_post", "open_photo":
	default:
		return ErrInvalid
	}
	return nil
}

func UUID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func UUIDValid(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, e := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return e == nil
}

func NormalizeVector(v []float64) ([]float64, error) {
	if len(v) != Dimensions {
		return nil, ErrInvalid
	}
	var norm float64
	for _, x := range v {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, ErrInvalid
		}
		norm = math.Hypot(norm, x)
	}
	if norm < 1e-12 {
		return nil, ErrInvalid
	}
	if math.IsInf(norm, 0) {
		return nil, ErrInvalid
	}
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = x / norm
	}
	return out, nil
}

func Vector(v []float64) (string, error) {
	v, err := NormalizeVector(v)
	if err != nil {
		return "", err
	}
	s := make([]string, len(v))
	for i, x := range v {
		s[i] = strconv.FormatFloat(x, 'g', 9, 64)
	}
	return "[" + strings.Join(s, ",") + "]", nil
}

func ParseVector(s string) []float64 {
	parts := strings.Split(strings.Trim(s, "[]"), ",")
	v := make([]float64, len(parts))
	for i, p := range parts {
		v[i], _ = strconv.ParseFloat(p, 64)
	}
	return v
}
func Cosine(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, aa, bb float64
	for i, x := range a {
		dot += x * b[i]
		aa += x * x
		bb += b[i] * b[i]
	}
	if aa*bb < 1e-12 {
		return 0
	}
	return dot / math.Sqrt(aa*bb)
}
