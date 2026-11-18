package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func unitNames(names []string) []string {
	if names == nil {
		return []string{}
	}
	return names
}

func auditSource(r *http.Request) (domain.Source, bool) {
	source := domain.Source(r.URL.Query().Get("source"))
	switch source {
	case "", domain.SourceBMKG, domain.SourcePVMBG, domain.SourceSatellite, domain.SourceCoastal:
		return source, true
	default:
		return "", false
	}
}

func (s *Server) auditEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	source, ok := auditSource(r)
	if !ok {
		obs.WriteError(w, 400, "bad_request", "unsupported source")
		return
	}
	f := ports.ListFilter{AuditOrder: true, Source: source, Cursor: q.Get("cursor")}
	if f.Cursor != "" {
		if _, id, err := ports.DecodeCursor(f.Cursor); err != nil || id == "" {
			obs.WriteError(w, 400, "bad_request", "invalid cursor")
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			obs.WriteError(w, 400, "bad_request", "limit must be a positive integer")
			return
		}
		f.Limit = n
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			obs.WriteError(w, 400, "bad_request", "since must be ISO 8601")
			return
		}
		f.Since = &t
	}
	page, err := s.Repo.List(r.Context(), f)
	if err != nil {
		obs.WriteError(w, 503, "store_unavailable", "canonical store unavailable")
		return
	}
	items := page.Items
	if items == nil {
		items = []domain.HazardEvent{}
	}
	var next *string
	if page.NextCursor != "" {
		next = &page.NextCursor
	}
	obs.WriteJSON(w, 200, map[string]any{
		"items": items, "next_cursor": next, "partial": len(page.UnavailableUnits) > 0,
		"unavailable_units": unitNames(page.UnavailableUnits),
	})
}

func (s *Server) auditChecksum(w http.ResponseWriter, r *http.Request) {
	source, ok := auditSource(r)
	if !ok {
		obs.WriteError(w, 400, "bad_request", "unsupported source")
		return
	}
	refs, units, err := auditReferences(r.Context(), s.Repo, source)
	if err != nil {
		obs.WriteError(w, 503, "store_unavailable", "canonical store unavailable")
		return
	}
	slices.Sort(refs)
	digest := sha256.New()
	for i, ref := range refs {
		if i > 0 {
			_, _ = io.WriteString(digest, "\n")
		}
		_, _ = io.WriteString(digest, ref)
	}
	slices.Sort(units)
	obs.WriteJSON(w, 200, map[string]any{
		"source": source, "count": len(refs), "checksum": hex.EncodeToString(digest.Sum(nil)),
		"algorithm": "sha256", "as_of": s.now().UTC(), "partial": len(units) > 0, "unavailable_units": unitNames(units),
	})
}

func auditReferences(ctx context.Context, repo ports.Repository, source domain.Source) ([]string, []string, error) {
	if indexed, ok := repo.(ports.AuditReferences); ok {
		return indexed.AuditReferences(ctx, source)
	}
	f := ports.ListFilter{AuditOrder: true, Source: source, Limit: 1000}
	var refs []string
	missing := map[string]bool{}
	for {
		page, err := repo.List(ctx, f)
		if err != nil {
			return nil, nil, err
		}
		for _, h := range page.Items {
			refs = append(refs, h.SourceRefID)
		}
		for _, unit := range page.UnavailableUnits {
			missing[unit] = true
		}
		f.Cursor = page.NextCursor
		if f.Cursor == "" {
			break
		}
	}
	units := make([]string, 0, len(missing))
	for unit := range missing {
		units = append(units, unit)
	}
	return refs, units, nil
}

func (s *Server) auditLocation(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("hazard_id")
	if id == "" {
		obs.WriteError(w, 400, "bad_request", "hazard_id is required")
		return
	}
	locations, partial, err := []ports.Location{}, false, error(nil)
	if locator, ok := s.Repo.(ports.Locator); ok {
		locations, partial, err = locator.Locate(r.Context(), id)
	} else {
		var h *domain.HazardEvent
		h, err = s.Repo.Get(r.Context(), id)
		if err == nil && h.Retired {
			err = ports.ErrNotFound
		}
		if err == nil {
			if node := os.Getenv("CANONICAL_STORE_NODE"); node != "" {
				locations = append(locations, ports.Location{Node: node, Unit: "canonical", Role: "primary"})
			} else {
				partial = true
			}
		}
	}
	switch {
	case errors.Is(err, ports.ErrNotFound):
		obs.WriteError(w, 404, "not_found", "hazard not found")
	case err != nil:
		obs.WriteError(w, 503, "store_unavailable", "canonical store unavailable")
	default:
		obs.WriteJSON(w, 200, map[string]any{"hazard_id": id, "locations": locations, "partial": partial})
	}
}
