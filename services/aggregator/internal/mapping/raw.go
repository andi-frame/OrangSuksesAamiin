// Package mapping turns raw BMKG/PVMBG JSON records into canonical HazardEvents.
//
// SCHEMA-EVOLUTION POLICY (Problem 1) - "tolerant reader, additive-only":
//
//  1. A source record is decoded into a generic map, never into a fixed struct,
//     so a field this code has never heard of can neither be dropped silently nor
//     make the whole message fail to parse.
//  2. Fields that have a canonical counterpart are read explicitly and validated
//     (required + type). A missing or wrongly-typed required field, or an
//     unknown enum value, is NOT guessed: the record is quarantined (logged,
//     counted, kept in /internal/quarantine) and the rest of the batch goes on.
//  3. EVERY other top-level field - known-but-attribute-only ones as well as
//     fields that appear later (e.g. confidence_level) - is forwarded verbatim
//     into HazardEvent.attributes. Nothing is ignored, nothing needs a redeploy.
//
// Supported changes: new fields (any JSON type), disappearance of attribute-only
// fields. Consciously unsupported (-> quarantine): rename/removal of a field
// with a canonical counterpart, type change of such a field, new enum values,
// changed semantics/units.
//
// AI-assisted (Claude Code): see AI_USAGE.md. Reviewed and understood by the team.
package mapping

import (
	"fmt"
	"strings"
	"time"
)

// MappingError marks a record that cannot be mapped (-> quarantine). It is
// distinct from a source failure (network, 5xx), which is the breaker's concern.
type MappingError struct {
	Field  string
	Reason string
}

func (e *MappingError) Error() string { return e.Field + ": " + e.Reason }

func bad(field, format string, args ...any) *MappingError {
	return &MappingError{Field: field, Reason: fmt.Sprintf(format, args...)}
}

func reqString(m map[string]any, k string) (string, error) {
	v, ok := m[k]
	if !ok || v == nil {
		return "", bad(k, "required field is missing")
	}
	s, ok := v.(string)
	if !ok {
		return "", bad(k, "expected string, got %T", v)
	}
	if strings.TrimSpace(s) == "" {
		return "", bad(k, "must not be empty")
	}
	return s, nil
}

func reqFloat(m map[string]any, k string) (float64, error) {
	v, ok := m[k]
	if !ok || v == nil {
		return 0, bad(k, "required field is missing")
	}
	f, ok := v.(float64)
	if !ok {
		return 0, bad(k, "expected number, got %T", v)
	}
	return f, nil
}

func reqTime(m map[string]any, k string) (time.Time, error) {
	s, err := reqString(m, k)
	if err != nil {
		return time.Time{}, err
	}
	t, perr := time.Parse(time.RFC3339Nano, s)
	if perr != nil {
		return time.Time{}, bad(k, "not an ISO 8601 datetime: %q", s)
	}
	// microsecond precision survives a PostgreSQL round-trip unchanged
	return t.UTC().Truncate(time.Microsecond), nil
}

// optTime parses an optional datetime; ok=false when absent or unparsable.
func optTime(m map[string]any, k string) (time.Time, bool) {
	s, isStr := m[k].(string)
	if !isStr {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC().Truncate(time.Microsecond), true
}

// splitExtras implements rule 3 of the policy: everything that is not in known
// is copied to the attributes map unchanged.
func splitExtras(raw map[string]any, known map[string]struct{}) map[string]any {
	extras := make(map[string]any)
	for k, v := range raw {
		if _, ok := known[k]; !ok {
			extras[k] = v
		}
	}
	return extras
}

func set(keys ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

// RecordID returns a best-effort identifier of a raw record for log/quarantine.
func RecordID(raw map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := raw[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
