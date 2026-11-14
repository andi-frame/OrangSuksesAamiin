package sources

import (
	"encoding/json"
	"strings"
	"testing"
)

func split(t *testing.T, body string, lim Limits) []Element {
	t.Helper()
	els, err := SplitArray(strings.NewReader(body), lim)
	if err != nil {
		t.Fatal(err)
	}
	return els
}

func problems(els []Element) []string {
	out := make([]string, len(els))
	for i, e := range els {
		out[i] = e.Problem
	}
	return out
}

func TestSplitKeepsExactBytesOfValidElements(t *testing.T) {
	body := " [ {\"a\":\"x]}\\\"{\" , \"n\":[1,{\"b\":null}]} ,\n\"str]\" , 12.5e3,true, {} ]  "
	els := split(t, body, Limits{})
	want := []string{`{"a":"x]}\"{" , "n":[1,{"b":null}]}`, `"str]"`, `12.5e3`, `true`, `{}`}
	if len(els) != len(want) {
		t.Fatalf("got %d elements: %+v", len(els), els)
	}
	for i, w := range want {
		if string(els[i].Raw) != w || els[i].Problem != "" || els[i].Size != int64(len(w)) {
			t.Fatalf("element %d = %q (%q), want %q", i, els[i].Raw, els[i].Problem, w)
		}
		if !json.Valid(els[i].Raw) {
			t.Fatalf("element %d not valid JSON", i)
		}
	}
	if got := split(t, "[]", Limits{}); len(got) != 0 {
		t.Fatalf("empty array: %+v", got)
	}
}

func TestSplitIsolatesEveryKindOfBrokenElement(t *testing.T) {
	deep := `{"report_id":"VR-deep","x":` + strings.Repeat("[", 200000) + strings.Repeat("]", 200000) + `}`
	huge := `{"report_id":"VR-huge","blob":"` + strings.Repeat("A", 3<<20) + `"}`
	bad := "{\"report_id\":\"VR-utf8\",\"name\":\"Merapi \xff\xfe\"}"
	body := `[{"report_id":"ok-1"},` + deep + `,{"report_id":"ok-2"},` + huge + `,` + bad + `,{"report_id":"ok-3"}]`
	els := split(t, body, Limits{MaxBytes: 1 << 20, MaxDepth: 32})
	got := problems(els)
	want := []string{"", ProblemTooDeep, "", ProblemTooLarge, ProblemInvalidUTF8, ""}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("problems = %v, want %v", got, want)
	}
	if string(els[5].Raw) != `{"report_id":"ok-3"}` {
		t.Fatalf("element after the broken ones: %q", els[5].Raw)
	}
	if len(els[3].Raw) != 1<<20 || els[3].Size != int64(len(huge)) {
		t.Fatalf("too large: kept %d bytes of %d", len(els[3].Raw), els[3].Size)
	}
	if string(els[1].Raw) != deep {
		t.Fatal("a deep element within the size limit must be kept whole")
	}
	if string(els[4].Raw) != bad {
		t.Fatal("invalid UTF-8 must be kept byte for byte")
	}
}

func TestSplitTruncatedBodyKeepsCompleteElements(t *testing.T) {
	els := split(t, `[{"report_id":"a"},{"report_id":"b","alert_lev`, Limits{})
	if len(els) != 2 || els[0].Problem != "" || els[1].Problem != ProblemTruncated ||
		string(els[1].Raw) != `{"report_id":"b","alert_lev` {
		t.Fatalf("got %+v", els)
	}
	// cut right after a comma
	if els := split(t, `[{"a":1},`, Limits{}); len(els) != 2 || els[1].Problem != ProblemTruncated {
		t.Fatalf("cut after comma: %+v", els)
	}
	// cut inside a string
	if els := split(t, `["abc`, Limits{}); len(els) != 1 || els[0].Problem != ProblemTruncated {
		t.Fatalf("cut in string: %+v", els)
	}
	// array never closed after a complete element: nothing is lost, nothing to report
	if els := split(t, `[{"a":1}`, Limits{}); len(els) != 1 || els[0].Problem != "" {
		t.Fatalf("unclosed array: %+v", els)
	}
}

func TestSplitNonArrayAndGarbage(t *testing.T) {
	if els := split(t, `{"items":[]}`, Limits{}); len(els) != 1 || els[0].Problem != ProblemNotAnArray ||
		string(els[0].Raw) != `{"items":[]}` {
		t.Fatalf("object body: %+v", els)
	}
	if els := split(t, "", Limits{}); len(els) != 1 || els[0].Problem != ProblemNotAnArray {
		t.Fatalf("empty body: %+v", els)
	}
	els := split(t, `[{"a":1} xx {"b":2}]`, Limits{})
	if len(els) != 2 || els[0].Problem != "" || els[1].Problem != ProblemGarbage {
		t.Fatalf("garbage: %+v", els)
	}
}
