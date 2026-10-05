package token_test

import (
	"context"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/store"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/token"
)

func TestIssueRefreshRotate(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := config.Config{
		JWTSecret:  []byte("test-secret"),
		AccessTTL:  time.Minute,
		RefreshTTL: time.Hour,
		Clients: map[string]config.Client{
			"media-client": {ID: "media-client", Secret: "secret", Role: "media"},
		},
	}
	svc := token.New(cfg, st)
	ctx := context.Background()

	p1, err := svc.Issue(ctx, "media-client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	role, err := token.ParseAccess(cfg.JWTSecret, p1.AccessToken)
	if err != nil || role != "media" {
		t.Fatalf("parse: role=%q err=%v", role, err)
	}

	p2, err := svc.Refresh(ctx, p1.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if p2.AccessToken == p1.AccessToken {
		t.Fatal("expected new access token after refresh")
	}
	if _, err := svc.Refresh(ctx, p1.RefreshToken); err == nil {
		t.Fatal("old refresh token should be rejected after rotate")
	}
}
