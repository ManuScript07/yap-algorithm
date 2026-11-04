package cursor

import (
	"testing"
	"yap-algorithm/internal/domain"
)

func TestCursorBindingAndTamper(t *testing.T) {
	c := Codec{Key: []byte("a sufficiently long test secret 123456")}
	u := domain.UUID()
	v := Value{SessionID: domain.UUID(), UserID: u, Tab: "for_you", Offset: 20, Limit: 20}
	token := c.Encode(v)
	if got, e := c.Decode(token, u, "for_you", 20); e != nil || got != v {
		t.Fatal(got, e)
	}
	for _, input := range []struct {
		token, user, tab string
		limit            int
	}{{token, domain.UUID(), "for_you", 20}, {token, u, "friends", 20}, {token, u, "for_you", 30}, {token + "x", u, "for_you", 20}} {
		if _, e := c.Decode(input.token, input.user, input.tab, input.limit); e == nil {
			t.Fatal("invalid cursor accepted")
		}
	}
}
