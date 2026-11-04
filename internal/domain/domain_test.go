package domain

import (
	"math"
	"strings"
	"testing"
)

func TestInputBoundaries(t *testing.T) {
	for _, l := range []Location{{91, 0}, {0, 181}, {math.Inf(1), 0}} {
		if l.Valid() {
			t.Fatal("invalid location")
		}
	}
	p := NewPost{Text: "hello", Audience: "public", Language: "en", Photos: []string{"a", "b", "c", "d"}}
	if p.Validate() == nil {
		t.Fatal("four images accepted")
	}
	v := make([]float64, Dimensions)
	v[0] = math.NaN()
	if _, e := Vector(v); e == nil {
		t.Fatal("nonfinite vector")
	}
	e := Event{ID: UUID(), SessionID: UUID(), PostID: UUID(), Kind: "impression", VisibleMS: 999, VisibleFraction: 1}
	if e.Validate() == nil {
		t.Fatal("unqualified impression")
	}
	e.VisibleMS = 1000
	if e.Validate() != nil {
		t.Fatal("valid impression rejected")
	}
}

func TestPostUnicodeTextLimit(t *testing.T) {
	p := NewPost{Text: strings.Repeat("😀", 1000), Audience: "public", Language: "en"}
	if err := p.Validate(); err != nil {
		t.Fatal("1000 Unicode characters rejected")
	}
	p.Text += "я"
	if err := p.Validate(); err == nil {
		t.Fatal("1001 characters accepted")
	}
}
