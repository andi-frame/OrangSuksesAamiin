// Command pvmbg-token mints a PVMBG access token for the BNPB gateway.
//
// The token format is defined by the PVMBG mock (services/pvmbg-mock/internal/
// pvmbg/token.go): pvmbg.v1.<claims>.<hmac>. This tool is a standalone copy so
// that no code is shared between services.
//
//	PVMBG_TOKEN_SECRET=... go run ./tools/pvmbg-token [-sub bnpb-aggregator] [-ttl 8760h]
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	sub := flag.String("sub", "bnpb-aggregator", "token subject")
	ttl := flag.Duration("ttl", 10*365*24*time.Hour, "token lifetime")
	flag.Parse()

	secret := os.Getenv("PVMBG_TOKEN_SECRET")
	if secret == "" {
		fmt.Fprintln(os.Stderr, "PVMBG_TOKEN_SECRET is not set")
		os.Exit(2)
	}
	now := time.Now()
	c, _ := json.Marshal(map[string]any{
		"iss": "pvmbg-geologi", "aud": "pvmbg-api", "sub": *sub,
		"iat": now.Unix(), "exp": now.Add(*ttl).Unix(),
	})
	input := "pvmbg.v1." + base64.RawURLEncoding.EncodeToString(c)
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(input))
	fmt.Println(input + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil)))
}
