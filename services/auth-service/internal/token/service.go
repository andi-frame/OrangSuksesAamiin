package token

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/store"
)

var (
	ErrInvalidClient  = errors.New("invalid client credentials")
	ErrInvalidRefresh = errors.New("invalid refresh token")
)

type Pair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Role         string `json:"role"`
}

type Service struct {
	cfg   config.Config
	store *store.Store
	now   func() time.Time
}

func New(cfg config.Config, st *store.Store) *Service {
	return &Service{cfg: cfg, store: st, now: time.Now}
}

func (s *Service) Issue(ctx context.Context, clientID, secret string) (Pair, error) {
	c, ok := s.cfg.Clients[clientID]
	if !ok || !secretEqual(c.Secret, secret) {
		return Pair{}, ErrInvalidClient
	}
	return s.mint(ctx, c.ID, c.Role)
}

func (s *Service) Refresh(ctx context.Context, refreshPlain string) (Pair, error) {
	if refreshPlain == "" {
		return Pair{}, ErrInvalidRefresh
	}
	clientID, role, err := s.store.ConsumeRefresh(ctx, hashToken(refreshPlain))
	if err != nil {
		return Pair{}, ErrInvalidRefresh
	}
	return s.mint(ctx, clientID, role)
}

func (s *Service) mint(ctx context.Context, clientID, role string) (Pair, error) {
	now := s.now().UTC()
	exp := now.Add(s.cfg.AccessTTL)

	jti, err := randomHex(8)
	if err != nil {
		return Pair{}, err
	}
	claims := jwt.MapClaims{
		"sub":  clientID,
		"role": role,
		"jti":  jti,
		"iat":  now.Unix(),
		"exp":  exp.Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	access, err := tok.SignedString(s.cfg.JWTSecret)
	if err != nil {
		return Pair{}, fmt.Errorf("sign access: %w", err)
	}

	refreshPlain, err := randomHex(32)
	if err != nil {
		return Pair{}, err
	}
	if err := s.store.SaveRefresh(ctx, hashToken(refreshPlain), clientID, role, now.Add(s.cfg.RefreshTTL)); err != nil {
		return Pair{}, fmt.Errorf("save refresh: %w", err)
	}

	return Pair{
		AccessToken:  access,
		RefreshToken: refreshPlain,
		TokenType:    "Bearer",
		ExpiresIn:    int(s.cfg.AccessTTL.Seconds()),
		Role:         role,
	}, nil
}

func ParseAccess(secret []byte, access string) (role string, err error) {
	parsed, err := jwt.Parse(access, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	if err != nil || !parsed.Valid {
		return "", errors.New("invalid access token")
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return "", errors.New("invalid claims")
	}
	role, _ = claims["role"].(string)
	if role == "" {
		return "", errors.New("missing role claim")
	}
	return role, nil
}

func secretEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func hashToken(plain string) string {
	sum := sha256.Sum256([]byte(plain))
	return hex.EncodeToString(sum[:])
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
