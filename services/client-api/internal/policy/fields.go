package policy

import (
	"encoding/json"
	"errors"
	"strings"
)

var summaryKeys = map[string]bool{
	"hazard_id": true, "source": true, "hazard_type": true, "severity": true,
	"area_name": true, "occurred_at": true, "ingested_at": true, "version": true, "updated_at": true,
}

var mentahKeys = map[string]bool{
	"source_ref_id": true, "latitude": true, "longitude": true, "attributes": true,
}

var ErrForbiddenField = errors.New("field not allowed for role")

func CheckQuery(role, fieldsParam, includeParam string) error {
	if role != "media" {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(includeParam), "mentah") {
		return ErrForbiddenField
	}
	for _, f := range strings.Split(fieldsParam, ",") {
		f = strings.TrimSpace(f)
		if f != "" && mentahKeys[f] {
			return ErrForbiddenField
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
