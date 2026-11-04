package cursor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"yap-algorithm/internal/domain"
)

type Value struct {
	SessionID string `json:"s"`
	UserID    string `json:"u"`
	Tab       string `json:"t"`
	Offset    int    `json:"o"`
	Limit     int    `json:"l"`
}
type Codec struct{ Key []byte }

func (c Codec) Encode(v Value) string {
	b, _ := json.Marshal(v)
	m := hmac.New(sha256.New, c.Key)
	m.Write(b)
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}
func (c Codec) Decode(s, user, tab string, limit int) (Value, error) {
	var v Value
	if len(s) > 1000 {
		return v, domain.ErrInvalid
	}
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return v, domain.ErrInvalid
	}
	b, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return v, domain.ErrInvalid
	}
	sig, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return v, domain.ErrInvalid
	}
	m := hmac.New(sha256.New, c.Key)
	m.Write(b)
	if !hmac.Equal(sig, m.Sum(nil)) {
		return v, domain.ErrInvalid
	}
	if json.Unmarshal(b, &v) != nil || v.UserID != user || v.Tab != tab || v.Limit != limit || v.Offset < 0 || v.Offset > 2000 || !domain.UUIDValid(v.SessionID) {
		return v, domain.ErrInvalid
	}
	return v, nil
}
