// Package schemacheck is a tiny JSON Schema validator used by the contract tests
// (dependency-free on purpose). It supports the subset the files in
// contracts/schemas use: type, required, properties, additionalProperties,
// enum, items, minimum, maximum, minLength, pattern, format=date-time, $ref
// (to a sibling file) and nullable types (type: ["number","null"]).
//
// It is a test helper: no service imports it in production code.
package schemacheck

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"time"
)

// Schema is a loaded schema document.
type Schema struct {
	dir string
	doc map[string]any
}

// Load reads a schema file. $ref values are resolved relative to its directory.
func Load(path string) (*Schema, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &Schema{dir: filepath.Dir(path), doc: doc}, nil
}

// Validate checks a decoded JSON value (as produced by encoding/json into any)
// and returns every violation found ("$.hazard.severity: ...").
func (s *Schema) Validate(v any) []string {
	var errs []string
	s.check(s.doc, v, "$", &errs)
	return errs
}

// ValidateJSON decodes raw JSON and validates it.
func (s *Schema) ValidateJSON(raw []byte) []string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return []string{"invalid JSON: " + err.Error()}
	}
	return s.Validate(v)
}

// ValidateValue marshals any Go value to JSON first, then validates it.
func (s *Schema) ValidateValue(x any) []string {
	b, err := json.Marshal(x)
	if err != nil {
		return []string{"cannot marshal: " + err.Error()}
	}
	return s.ValidateJSON(b)
}

func typeOf(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		if t == float64(int64(t)) {
			return "integer"
		}
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return fmt.Sprintf("%T", v)
}

func typeOK(want string, v any) bool {
	got := typeOf(v)
	return got == want || (want == "number" && got == "integer")
}

func (s *Schema) check(sc map[string]any, v any, path string, errs *[]string) {
	if ref, ok := sc["$ref"].(string); ok {
		sub, err := Load(filepath.Join(s.dir, ref))
		if err != nil {
			*errs = append(*errs, path+": cannot resolve $ref "+ref+": "+err.Error())
			return
		}
		sub.check(sub.doc, v, path, errs)
		return
	}
	if t, ok := sc["type"]; ok {
		var types []string
		switch tt := t.(type) {
		case string:
			types = []string{tt}
		case []any:
			for _, x := range tt {
				types = append(types, x.(string))
			}
		}
		match := false
		for _, want := range types {
			if typeOK(want, v) {
				match = true
			}
		}
		if !match {
			*errs = append(*errs, fmt.Sprintf("%s: expected %v, got %s", path, types, typeOf(v)))
			return
		}
	}
	if en, ok := sc["enum"].([]any); ok {
		found := false
		for _, e := range en {
			if reflect.DeepEqual(e, v) {
				found = true
			}
		}
		if !found {
			*errs = append(*errs, fmt.Sprintf("%s: %v not in enum %v", path, v, en))
		}
	}
	switch x := v.(type) {
	case float64:
		if m, ok := sc["minimum"].(float64); ok && x < m {
			*errs = append(*errs, fmt.Sprintf("%s: %v < minimum %v", path, x, m))
		}
		if m, ok := sc["maximum"].(float64); ok && x > m {
			*errs = append(*errs, fmt.Sprintf("%s: %v > maximum %v", path, x, m))
		}
	case string:
		if m, ok := sc["minLength"].(float64); ok && float64(len(x)) < m {
			*errs = append(*errs, fmt.Sprintf("%s: shorter than minLength %v", path, m))
		}
		if p, ok := sc["pattern"].(string); ok && !regexp.MustCompile(p).MatchString(x) {
			*errs = append(*errs, fmt.Sprintf("%s: %q does not match %s", path, x, p))
		}
		if f, ok := sc["format"].(string); ok && f == "date-time" {
			if _, err := time.Parse(time.RFC3339Nano, x); err != nil {
				*errs = append(*errs, fmt.Sprintf("%s: %q is not an ISO 8601 date-time", path, x))
			}
		}
	case []any:
		if it, ok := sc["items"].(map[string]any); ok {
			for i, e := range x {
				s.check(it, e, fmt.Sprintf("%s[%d]", path, i), errs)
			}
		}
	case map[string]any:
		props, _ := sc["properties"].(map[string]any)
		if req, ok := sc["required"].([]any); ok {
			for _, r := range req {
				if _, has := x[r.(string)]; !has {
					*errs = append(*errs, fmt.Sprintf("%s: required property %q is missing", path, r))
				}
			}
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if p, ok := props[k].(map[string]any); ok {
				s.check(p, x[k], path+"."+k, errs)
			} else if ap, ok := sc["additionalProperties"].(bool); ok && !ap {
				*errs = append(*errs, fmt.Sprintf("%s: additional property %q not allowed", path, k))
			}
		}
	}
}
