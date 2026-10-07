package policy

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

var summaryKeys = map[string]bool{
	"hazard_id": true, "source": true, "hazard_type": true, "severity": true,
	"area_name": true, "occurred_at": true, "ingested_at": true, "version": true, "updated_at": true,
}

var ErrForbiddenField = errors.New("field not allowed for role")

// includeSummary are the only `include` values a Media client may send; anything else (mentah, raw, all, ...)
// is treated as an attempt to widen the response and is rejected rather than silently ignored.
var includeSummary = map[string]bool{"": true, "summary": true, "ringkasan": true}

// CheckQuery enforces the field classification for explicit requests. Media is whitelisted: every `fields`
// entry must be a Ringkasan field and `include` may only ask for the summary. Unknown names are rejected too
// (not just known Mentah names), so a renamed or re-cased Mentah field cannot slip through as a 200.
// Parameter spelling variants (`fields[]`, repeated keys, comma lists, case, padding) are all covered.
func CheckQuery(role string, q url.Values) error {
	if role != "media" {
		return nil
	}
	for key, values := range q {
		name := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(key), "[]"))
		switch name {
		case "fields":
			for _, v := range values {
				for _, f := range strings.Split(v, ",") {
					f = strings.ToLower(strings.TrimSpace(f))
					if f != "" && !summaryKeys[f] {
						return ErrForbiddenField
					}
				}
			}
		case "include":
			for _, v := range values {
				for _, f := range strings.Split(v, ",") {
					if !includeSummary[strings.ToLower(strings.TrimSpace(f))] {
						return ErrForbiddenField
					}
				}
			}
		}
	}
	return nil
}

func FilterItem(role string, raw json.RawMessage, stale bool) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	if role != "media" {
		m["stale"] = stale
		return m, nil
	}
	out := map[string]any{"stale": stale}
	for k, v := range m {
		if summaryKeys[k] {
			out[k] = v
		}
	}
	return out, nil
}

func CanSourcesStatus(role string) bool {
	return role == "pusat" || role == "lapangan"
}
