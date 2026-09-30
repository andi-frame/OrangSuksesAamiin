package obs

import (
	"os"
	"strconv"
	"time"
)

// EnvString returns $key or def.
func EnvString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt returns $key parsed as int, or def when unset/invalid.
func EnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// EnvBool returns $key parsed as bool, or def when unset/invalid.
func EnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// EnvSeconds returns $key (seconds, may be fractional) as a Duration.
func EnvSeconds(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			return time.Duration(f * float64(time.Second))
		}
	}
	return def
}

// EnvMillis returns $key (milliseconds) as a Duration.
func EnvMillis(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return def
}

// MustEnv returns $key or terminates the process with a clear message when it
// is empty. Used for credentials, which must never have hard-coded defaults.
func MustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		_, _ = os.Stderr.WriteString("FATAL: required environment variable " + key + " is not set (see .env.example)\n")
		os.Exit(2)
	}
	return v
}
