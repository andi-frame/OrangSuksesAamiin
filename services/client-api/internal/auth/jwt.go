package auth

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

func ParseBearer(secret []byte, bearer string) (role string, err error) {
	parsed, err := jwt.Parse(bearer, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil || !parsed.Valid {
		return "", errors.New("invalid token")
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return "", errors.New("invalid claims")
	}
	role, _ = claims["role"].(string)
	switch role {
	case "media", "lapangan", "pusat":
		return role, nil
	default:
		return "", errors.New("unknown role")
	}
}
