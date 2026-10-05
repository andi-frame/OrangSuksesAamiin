package dedup_test

import (
	"testing"

	"github.com/andi-frame/orangsuksesaamiin/services/consumer-hazard/internal/dedup"
)

func TestMarkIdempotent(t *testing.T) {
	s := dedup.New()
	if !s.Mark("a:1") {
		t.Fatal("first mark")
	}
	if s.Mark("a:1") {
		t.Fatal("duplicate must return false")
	}
	if s.Count() != 1 {
		t.Fatalf("count=%d", s.Count())
	}
}
