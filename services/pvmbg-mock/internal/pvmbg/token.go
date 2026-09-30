package pvmbg

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// PVMBG access tokens have their own structure, deliberately different from
// BMKG's opaque API key:
//
//	pvmbg.v1.<base64url(claims-json)>.<base64url(HMAC-SHA256(secret, "pvmbg.v1."+payload))>
//
// so a BMKG key can never be a valid PVMBG token (and vice versa).
const (
	tokenPrefix = "pvmbg.v1"
	issuer      = "pvmbg-geologi"
	audience    = "pvmbg-api"
)

type claims struct {
	Iss string `json:"iss"`
	Aud string `json:"aud"`
	Sub string `json:"sub"`
	Iat int64  `json:"iat"`
	Exp int64  `json:"exp"`
}

// Errors returned by VerifyToken. ErrExpired maps to 401, the others to 403.
var (
	ErrMalformed = errors.New("malformed token")
	ErrSignature = errors.New("invalid signature")
	ErrClaims    = errors.New("invalid claims")
	ErrExpired   = errors.New("token expired")
)

func sign(secret, signingInput string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(signingInput))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// MintToken creates a signed token for subject sub valid for ttl.
func MintToken(secret, sub string, ttl time.Duration, now time.Time) string {
	c, _ := json.Marshal(claims{Iss: issuer, Aud: audience, Sub: sub, Iat: now.Unix(), Exp: now.Add(ttl).Unix()})
	payload := base64.RawURLEncoding.EncodeToString(c)
	input := tokenPrefix + "." + payload
	return input + "." + sign(secret, input)
}

// VerifyToken validates structure, signature and claims.
func VerifyToken(secret, token string, now time.Time) error {
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0]+"."+parts[1] != tokenPrefix {
		return ErrMalformed
	}
	input := parts[0] + "." + parts[1] + "." + parts[2]
	if subtle.ConstantTimeCompare([]byte(sign(secret, input)), []byte(parts[3])) != 1 {
		return ErrSignature
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return ErrMalformed
	}
	var c claims
	if json.Unmarshal(raw, &c) != nil {
		return ErrMalformed
	}
	if c.Iss != issuer || c.Aud != audience {
		return ErrClaims
	}
	if now.Unix() >= c.Exp {
		return ErrExpired
	}
	return nil
}
