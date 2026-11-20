// Package reports is the write path of Tim Lapangan (M2 Problem 3, criterion 6):
// POST /v1/field-reports answers 202 Accepted quickly, even when the processing
// downstream is slow or dead, and no report that got a 202 is lost or processed
// twice.
//
//   - Durable before 202: the report is produced to the Kafka topic
//     "field-reports" with acks=all and the idempotent producer, and the 202 is
//     sent only after the broker acknowledged it. The Client-Facing API keeps
//     nothing in memory, so it may die at any moment after the 202. If the
//     broker does not acknowledge in time the client gets 503 and retries.
//   - Idempotent: the client sends an Idempotency-Key. The report id is derived
//     from (client id, key), so a retry of the same report (lost 202, timeout,
//     double tap) gets the same report_id and the same Kafka key; the processor
//     (services/field-reports) handles each report_id once.
//
// The delivery guarantee is therefore at-least-once into the broker plus an
// idempotent, transactional processor: effectively exactly-once processing.
package reports

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

// Topic names (created by the kafka-init job).
const (
	TopicIncoming  = "field-reports"
	TopicProcessed = "field-reports.processed"
)

// Envelope is the record produced to TopicIncoming.
type Envelope struct {
	ReportID       string          `json:"report_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	ClientID       string          `json:"client_id"`
	ReceivedAt     time.Time       `json:"received_at"`
	Report         json.RawMessage `json:"report"`
}

var keyRe = regexp.MustCompile(`^[A-Za-z0-9._:\-]{1,128}$`)

// ErrInvalid is a 400: the request can never succeed as sent.
type ErrInvalid struct{ Msg string }

func (e *ErrInvalid) Error() string { return e.Msg }

func invalid(format string, a ...any) error { return &ErrInvalid{Msg: fmt.Sprintf(format, a...)} }

// ReportID is deterministic per client and idempotency key.
func ReportID(clientID, key string) string {
	sum := sha256.Sum256([]byte(clientID + "\x00" + key))
	return "fr-" + hex.EncodeToString(sum[:])[:20]
}

// MaxBodyBytes bounds one report.
const MaxBodyBytes = 64 << 10

var (
	hazardTypes = map[string]bool{"SEISMIC": true, "VOLCANIC": true, "WILDFIRE": true, "COASTAL": true, "TSUNAMI": true, "OTHER": true}
	severities  = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}
)

// Validate checks the key and the body and returns the canonical JSON of the report.
//
// Body: {"area_name": string (required), "description": string (required, <= 4000),
// "hazard_type": SEISMIC|VOLCANIC|WILDFIRE|COASTAL|TSUNAMI|OTHER, "severity":
// NORMAL|WASPADA|SIAGA|AWAS, "latitude": number, "longitude": number,
// "observed_at": RFC 3339, "reporter": string}. Unknown fields are kept.
func Validate(key string, body []byte) (json.RawMessage, error) {
	if !keyRe.MatchString(key) {
		return nil, invalid("header Idempotency-Key is required: 1-128 characters [A-Za-z0-9._:-]")
	}
	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil || m == nil {
		return nil, invalid("body must be a JSON object")
	}
	if dec.More() {
		return nil, invalid("body must be a single JSON object")
	}
	str := func(k string, required bool, max int) error {
		v, ok := m[k]
		if !ok || v == nil {
			if required {
				return invalid("%s is required", k)
			}
			return nil
		}
		s, ok := v.(string)
		if !ok || (required && strings.TrimSpace(s) == "") || len(s) > max {
			return invalid("%s must be a non-empty string of at most %d characters", k, max)
		}
		return nil
	}
	for _, c := range []struct {
		k   string
		req bool
		max int
	}{{"area_name", true, 200}, {"description", true, 4000}, {"reporter", false, 200}} {
		if err := str(c.k, c.req, c.max); err != nil {
			return nil, err
		}
	}
	enum := func(k string, allowed map[string]bool) error {
		if v, ok := m[k]; ok && v != nil {
			s, ok := v.(string)
			if !ok || !allowed[strings.ToUpper(s)] {
				return invalid("%s has an unsupported value", k)
			}
			m[k] = strings.ToUpper(s)
		}
		return nil
	}
	if err := enum("hazard_type", hazardTypes); err != nil {
		return nil, err
	}
	if err := enum("severity", severities); err != nil {
		return nil, err
	}
	for k, lim := range map[string]float64{"latitude": 90, "longitude": 180} {
		if v, ok := m[k]; ok && v != nil {
			n, ok := v.(json.Number)
			f, err := n.Float64()
			if !ok || err != nil || math.IsNaN(f) || math.Abs(f) > lim {
				return nil, invalid("%s must be a number within +/-%g", k, lim)
			}
		}
	}
	if v, ok := m["observed_at"]; ok && v != nil {
		s, _ := v.(string)
		if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
			return nil, invalid("observed_at must be RFC 3339")
		}
	}
	out, err := json.Marshal(m)
	if err != nil {
		return nil, invalid("body cannot be encoded")
	}
	return out, nil
}

// Publisher hands a record to the broker and returns nil only once it is
// durably acknowledged.
type Publisher interface {
	Publish(ctx context.Context, key string, value []byte, headers map[string]string) error
}

// ErrUnavailable means the report was not accepted (the client must retry).
var ErrUnavailable = errors.New("report not accepted: broker unavailable")
