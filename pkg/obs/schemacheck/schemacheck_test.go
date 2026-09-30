package schemacheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestValidatorSubset(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "child.json", `{"type":"object","required":["x"],"properties":{"x":{"type":"integer","minimum":1}}}`)
	p := write(t, dir, "s.json", `{
	  "type":"object","required":["id","level","when","child"],"additionalProperties":false,
	  "properties":{
	    "id":{"type":"string","pattern":"^a-"},
	    "level":{"enum":["A","B"]},
	    "when":{"type":"string","format":"date-time"},
	    "score":{"type":["number","null"],"maximum":1},
	    "tags":{"type":"array","items":{"type":"string"}},
	    "child":{"$ref":"child.json"}
	  }}`)
	s, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	good := `{"id":"a-1","level":"A","when":"2026-10-01T10:00:00Z","score":null,"tags":["p"],"child":{"x":2}}`
	if errs := s.ValidateJSON([]byte(good)); len(errs) != 0 {
		t.Fatalf("valid document rejected: %v", errs)
	}
	bad := `{"id":"b-1","level":"C","when":"yesterday","score":3,"tags":[1],"child":{"x":0},"extra":true}`
	errs := strings.Join(s.ValidateJSON([]byte(bad)), "\n")
	for _, want := range []string{"does not match", "not in enum", "not an ISO 8601", "maximum", "expected [string]", "minimum", `additional property "extra"`} {
		if !strings.Contains(errs, want) {
			t.Errorf("missing violation %q in:\n%s", want, errs)
		}
	}
	if errs := s.ValidateJSON([]byte(`{}`)); len(errs) != 4 {
		t.Fatalf("expected 4 missing-property errors, got %v", errs)
	}
}
