package storage

import (
	"encoding/base64"
	"encoding/json"
)

func decodeClaims(payload string) (claims struct {
	Role    string `json:"role"`
	Expires int64  `json:"exp"`
}, err error) {
	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return claims, err
	}
	err = json.Unmarshal(data, &claims)
	return claims, err
}
