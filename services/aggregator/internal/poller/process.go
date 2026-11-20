package poller

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"unicode/utf8"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/dlq"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// Dead-letter kinds: what the raw bytes of a message are. The kind selects the
// handler that processes the message again.
const (
	KindPVMBGReport   = "pvmbg.volcanic-report"
	KindBMKGEvent     = "bmkg.seismic-event"
	KindBMKGWarning   = "bmkg.tsunami-warning"
	KindPVMBGFragment = "pvmbg.fragment" // an element cut off in transit
	KindBMKGFragment  = "bmkg.fragment"
)

// decodeObject is the only place raw bytes become a record. Every failure here
// is a property of the bytes, so it is permanent.
func decodeObject(raw []byte) (map[string]any, error) {
	if !utf8.Valid(raw) {
		return nil, dlq.Permanent("malformed: %s", sources.ProblemInvalidUTF8)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, dlq.Permanent("malformed: invalid_json: %v", err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, dlq.Permanent("schema_incompatible: record is not a JSON object")
	}
	return m, nil
}

// mappingFailure turns a mapping rejection into a permanent failure.
func mappingFailure(err error) error {
	var me *mapping.MappingError
	if errors.As(err, &me) {
		return dlq.Permanent("schema_incompatible: field '%s': %s", me.Field, me.Reason)
	}
	return err
}

var keyPattern = map[string]*regexp.Regexp{}

func init() {
	for _, f := range []string{"report_id", "event_id", "warning_id", "observation_id"} {
		keyPattern[f] = regexp.MustCompile(`"` + f + `"\s*:\s*"([^"\\]{1,200})"`)
	}
}

// recordKey reads the record identifier from bytes that may not parse at all
// (cut off, invalid UTF-8): it is what orders messages of the same record and
// what tells a fragment apart once the full record has arrived.
func recordKey(raw []byte, field string) string {
	if m := keyPattern[field].FindSubmatch(raw); m != nil && utf8.Valid(m[1]) {
		return string(m[1])
	}
	return ""
}

// elementFailure classifies an element the splitter already found broken.
func elementFailure(el sources.Element) *dlq.Failure {
	if el.Problem == sources.ProblemTruncated {
		return dlq.Transient("malformed: truncated in transit (%d bytes received); the source is polled again from the same watermark", el.Size)
	}
	if el.Problem == sources.ProblemTooLarge {
		return dlq.Permanent("malformed: too_large: %d bytes, limit %d (first %d bytes kept)", el.Size, len(el.Raw), len(el.Raw))
	}
	return dlq.Permanent("malformed: %s", el.Problem)
}

// FragmentHandler resolves a cut-off element once the record it belonged to
// has been stored from a later, complete response.
func FragmentHandler(repo ports.Repository, src domain.Source) dlq.Handler {
	return func(ctx context.Context, m ports.Unprocessed) error {
		if m.RecordKey == "" {
			return dlq.Transient("malformed: truncated in transit; no readable record id, redelivery cannot be confirmed")
		}
		_, err := repo.Get(ctx, domain.HazardID(src, m.RecordKey))
		if errors.Is(err, ports.ErrNotFound) {
			return dlq.Transient("malformed: truncated in transit; record %s not redelivered whole yet", m.RecordKey)
		}
		return err
	}
}
