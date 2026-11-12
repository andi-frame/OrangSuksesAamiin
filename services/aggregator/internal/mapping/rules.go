package mapping

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SchemaRules is how the system is adjusted to an incompatible source schema
// change without new code (M2 Problem 3). The mapping itself never guesses: a
// record that breaks the contract is parked. Once a person has looked at the
// parked messages and decided what the new field means, they declare it here
// (a ConfigMap), roll the Aggregator out and requeue the parked messages,
// which are then read through the rules.
//
// Rules run on the raw record before mapping. They only fill a canonical field
// that is absent, so a record already in the old schema is never altered.
//
//	{
//	  "PVMBG": {
//	    "rename": {"level": "alert_level", "location.volcano_code": "volcano_id"},
//	    "coerce": {"reported_at": "unix_seconds_to_rfc3339"}
//	  }
//	}
//
// rename: source path (dots address nested objects) -> top-level field.
// coerce: field -> conversion; see the Coerce* constants.
type SchemaRules map[string]SourceRules

// SourceRules are the rules of one source.
type SourceRules struct {
	Rename map[string]string `json:"rename,omitempty"`
	Coerce map[string]string `json:"coerce,omitempty"`
}

// Supported conversions.
const (
	CoerceUnixSeconds = "unix_seconds_to_rfc3339"
	CoerceUnixMillis  = "unix_millis_to_rfc3339"
	CoerceToString    = "number_to_string"
	CoerceToNumber    = "string_to_number"
)

// LoadSchemaRules reads the rules file; a missing file means "no rules".
func LoadSchemaRules(path string) (SchemaRules, error) {
	if path == "" {
		return SchemaRules{}, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return SchemaRules{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("schema rules: %w", err)
	}
	var r SchemaRules
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("schema rules %s: %w", path, err)
	}
	if err := r.Validate(); err != nil {
		return nil, fmt.Errorf("schema rules %s: %w", path, err)
	}
	return r, nil
}

// Validate rejects unknown conversions and empty names, so a typo stops the
// rollout (readiness never turns green) instead of silently changing nothing.
func (r SchemaRules) Validate() error {
	for src, sr := range r {
		for from, to := range sr.Rename {
			if from == "" || to == "" || strings.Contains(to, ".") {
				return fmt.Errorf("%s: invalid rename %q -> %q", src, from, to)
			}
		}
		for field, conv := range sr.Coerce {
			switch conv {
			case CoerceUnixSeconds, CoerceUnixMillis, CoerceToString, CoerceToNumber:
			default:
				return fmt.Errorf("%s: field %q: unknown conversion %q", src, field, conv)
			}
			if field == "" {
				return fmt.Errorf("%s: empty field in coerce", src)
			}
		}
	}
	return nil
}

// Summary lists the rules in a stable order (logs, /internal/ops).
func (r SchemaRules) Summary() []string {
	var out []string
	for src, sr := range r {
		for from, to := range sr.Rename {
			out = append(out, fmt.Sprintf("%s rename %s -> %s", src, from, to))
		}
		for f, c := range sr.Coerce {
			out = append(out, fmt.Sprintf("%s coerce %s %s", src, f, c))
		}
	}
	sort.Strings(out)
	return out
}

// Apply rewrites raw in place and returns what it changed.
func (r SchemaRules) Apply(source string, raw map[string]any) []string {
	sr, ok := r[source]
	if !ok {
		return nil
	}
	var applied []string
	froms := make([]string, 0, len(sr.Rename))
	for from := range sr.Rename {
		froms = append(froms, from)
	}
	sort.Strings(froms)
	for _, from := range froms {
		to := sr.Rename[from]
		if _, present := raw[to]; present {
			continue
		}
		if v, ok := takePath(raw, from); ok {
			raw[to] = v
			applied = append(applied, "rename "+from+"->"+to)
		}
	}
	fields := make([]string, 0, len(sr.Coerce))
	for f := range sr.Coerce {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	for _, f := range fields {
		if nv, ok := coerce(raw[f], sr.Coerce[f]); ok {
			raw[f] = nv
			applied = append(applied, "coerce "+f+" "+sr.Coerce[f])
		}
	}
	return applied
}

// takePath removes and returns the value at a dotted path; an emptied parent
// object is removed too.
func takePath(m map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	cur := m
	parents := []map[string]any{m}
	for _, p := range parts[:len(parts)-1] {
		next, ok := cur[p].(map[string]any)
		if !ok {
			return nil, false
		}
		cur = next
		parents = append(parents, cur)
	}
	last := parts[len(parts)-1]
	v, ok := cur[last]
	if !ok {
		return nil, false
	}
	delete(cur, last)
	for i := len(parents) - 1; i > 0; i-- {
		if len(parents[i]) == 0 {
			delete(parents[i-1], parts[i-1])
		}
	}
	return v, true
}

func coerce(v any, conv string) (any, bool) {
	switch conv {
	case CoerceUnixSeconds, CoerceUnixMillis:
		f, ok := v.(float64)
		if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		var t time.Time
		if conv == CoerceUnixSeconds {
			sec, frac := math.Modf(f)
			t = time.Unix(int64(sec), int64(frac*1e9))
		} else {
			t = time.UnixMilli(int64(f))
		}
		return t.UTC().Format(time.RFC3339Nano), true
	case CoerceToString:
		f, ok := v.(float64)
		if !ok {
			return nil, false
		}
		return strconv.FormatFloat(f, 'f', -1, 64), true
	case CoerceToNumber:
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return nil, false
		}
		return f, true
	}
	return nil, false
}
