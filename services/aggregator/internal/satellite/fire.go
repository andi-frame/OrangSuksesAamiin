package satellite

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
)

type detection struct {
	Key           string
	Satellite     string
	At            time.Time
	Lat, Lon, FRP float64
	Confidence    string
}

type province struct {
	Name     string
	Lat, Lon float64
}

type state struct {
	Offset     *int64
	Applied    *int64
	Latest     time.Time
	Late       int64
	Rejected   int64 // events whose content could not be applied (logged and skipped)
	Detections map[string]detection
	Inactive   map[string]detection
	Excluded   map[string]bool
	// Assignment also retains singletons, which have no fire ID.
	Assignment map[string]string
	Membership map[string][]string
	Fires      map[string]domain.HazardEvent
	Pending    []domain.HazardEvent
}

func newState() state {
	return state{Detections: map[string]detection{}, Assignment: map[string]string{},
		Inactive: map[string]detection{}, Excluded: map[string]bool{}, Membership: map[string][]string{}, Fires: map[string]domain.HazardEvent{}}
}

type cell struct{ X, Y, Z, T int64 }

func (d detection) cell() cell {
	// Neighboring points within 1 km are also within 1 km on every ECEF
	// axis. This grid works at the antimeridian and close to the poles.
	lat, lon := d.Lat*math.Pi/180, d.Lon*math.Pi/180
	const r = 6371.0088
	return cell{int64(math.Floor(r * math.Cos(lat) * math.Cos(lon))),
		int64(math.Floor(r * math.Cos(lat) * math.Sin(lon))),
		int64(math.Floor(r * math.Sin(lat))), d.At.Unix() / 21600}
}

func distance(lat, lon, otherLat, otherLon float64) float64 {
	phi, otherPhi := lat*math.Pi/180, otherLat*math.Pi/180
	a := math.Pow(math.Sin((otherPhi-phi)/2), 2) +
		math.Cos(phi)*math.Cos(otherPhi)*math.Pow(math.Sin((otherLon-lon)*math.Pi/360), 2)
	return 6371.0088 * 2 * math.Asin(math.Sqrt(min(1, a)))
}

type processor struct {
	state     *state
	grid      map[cell]map[string]bool
	provinces []province
	late      time.Duration
	session   string
}

func newProcessor(s *state, provinces []province, late time.Duration, session string) *processor {
	p := &processor{state: s, grid: map[cell]map[string]bool{}, provinces: provinces, late: late, session: session}
	for _, d := range s.Detections {
		p.add(d)
	}
	return p
}

func (p *processor) add(d detection) {
	c := d.cell()
	if p.grid[c] == nil {
		p.grid[c] = map[string]bool{}
	}
	p.grid[c][d.Key] = true
	p.state.Detections[d.Key] = d
}

func (p *processor) neighbors(d detection, visit func(string)) {
	c := d.cell()
	for x := int64(-1); x <= 1; x++ {
		for y := int64(-1); y <= 1; y++ {
			for z := int64(-1); z <= 1; z++ {
				for t := int64(-1); t <= 1; t++ {
					for key := range p.grid[cell{c.X + x, c.Y + y, c.Z + z, c.T + t}] {
						n := p.state.Detections[key]
						if n.At.Sub(d.At).Abs() <= 6*time.Hour && distance(d.Lat, d.Lon, n.Lat, n.Lon) <= 1 {
							visit(key)
						}
					}
				}
			}
		}
	}
}

func (p *processor) apply(e event) error {
	s := p.state
	if e.kind == "heartbeat" {
		return nil
	}
	d, oldKey, err := p.decodeChange(e.kind, e.data)
	if err != nil {
		return err
	}
	if oldKey == "" {
		oldKey = d.Key
	}
	if s.Excluded[oldKey] {
		return nil
	}
	if e.kind == "detection" {
		if _, exists := s.Detections[d.Key]; exists {
			return nil
		}
		if _, exists := s.Inactive[d.Key]; exists {
			return nil
		}
		if !s.Latest.IsZero() && d.At.Before(s.Latest.Add(-p.late)) {
			s.Excluded[d.Key] = true
			s.Late++
			return nil
		}
	}
	if d.At.After(s.Latest) {
		s.Latest = d.At
	}
	delete(s.Inactive, oldKey)
	if e.kind == "retract" {
		s.Excluded[oldKey] = true
	} else if d.Key == "" {
		return nil
	} else if d.Confidence == "l" {
		s.Inactive[d.Key] = d
	}
	old, exists := s.Detections[oldKey]
	seed := map[string]bool{}
	if exists {
		seed[oldKey] = true
		for _, key := range s.Membership[s.Assignment[oldKey]] {
			seed[key] = true
		}
		bucket := p.grid[old.cell()]
		delete(bucket, oldKey)
		if len(bucket) == 0 {
			delete(p.grid, old.cell())
		}
		delete(s.Detections, oldKey)
	}
	present := d.Key != "" && d.Confidence != "l"
	if present {
		if d.Key != oldKey && s.Assignment[d.Key] == "" {
			s.Assignment[d.Key] = s.Assignment[oldKey]
		}
		p.add(d)
		seed[d.Key] = true
	}
	p.rebuild(seed, e)
	if !present || oldKey != d.Key {
		delete(s.Assignment, oldKey)
	}
	return nil
}

// Rebuild only components reached from the removed detection's previous
// members or the added detection. Allocate survivor IDs before retiring losers.
func (p *processor) rebuild(seed map[string]bool, e event) {
	s := p.state
	visited := map[string]bool{}
	old := map[string]bool{}
	var components [][]string
	for start := range seed {
		if ref := s.Assignment[start]; ref != "" {
			old[ref] = true
		}
		if _, exists := s.Detections[start]; !exists || visited[start] {
			continue
		}
		visited[start] = true
		queue := []string{start}
		for i := 0; i < len(queue); i++ {
			p.neighbors(s.Detections[queue[i]], func(key string) {
				if !visited[key] {
					visited[key] = true
					queue = append(queue, key)
				}
			})
		}
		slices.Sort(queue)
		components = append(components, queue)
	}
	slices.SortFunc(components, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
	used := map[string]bool{}
	newMembership := map[string][]string{}
	for _, keys := range components {
		var candidates []string
		for _, key := range keys {
			if ref := s.Assignment[key]; ref != "" {
				old[ref] = true
				if !used[ref] {
					candidates = append(candidates, ref)
				}
			}
		}
		if len(keys) < 2 {
			continue
		}
		slices.Sort(candidates)
		var ref string
		if len(candidates) > 0 {
			ref = candidates[0]
		} else {
			sum := sha256.Sum256([]byte(p.session + "\x00" + e.id + "\x00" + keys[0]))
			ref = "fire-" + hex.EncodeToString(sum[:12])
		}
		used[ref] = true
		newMembership[ref] = keys
		p.record(ref, p.canonical(ref, keys), e.received)
	}
	var retired []string
	for ref := range old {
		delete(s.Membership, ref)
		if !used[ref] {
			retired = append(retired, ref)
		}
	}
	slices.Sort(retired)
	for _, ref := range retired {
		h := s.Fires[ref]
		h.Retired = true
		h.Attributes = map[string]any{"detection_count": 0.0, "detection_keys": []string{}, "satellites": []string{},
			"max_confidence": "", "total_frp_mw": 0.0}
		p.record(ref, h, e.received)
	}
	for key := range visited {
		delete(s.Assignment, key)
	}
	for ref, keys := range newMembership {
		s.Membership[ref] = keys
		for _, key := range keys {
			s.Assignment[key] = ref
		}
	}
}

func (p *processor) record(ref string, h domain.HazardEvent, at time.Time) {
	// Normalize JSON values exactly like PostgreSQL and the memory adapter.
	h = h.Clone()
	current := p.state.Fires[ref]
	if current.Version > 0 && domain.SameContent(current, h) {
		return
	}
	h.Version = current.Version + 1
	h.IngestedAt = current.IngestedAt
	if h.IngestedAt.IsZero() {
		h.IngestedAt = at
	}
	h.UpdatedAt = at
	p.state.Fires[ref] = h
	p.state.Pending = append(p.state.Pending, h)
}

func (p *processor) canonical(ref string, keys []string) domain.HazardEvent {
	var x, y, z, frp float64
	var first time.Time
	var satellites []string
	confidence := "n"
	for _, key := range keys {
		d := p.state.Detections[key]
		lat, lon := d.Lat*math.Pi/180, d.Lon*math.Pi/180
		x += math.Cos(lat) * math.Cos(lon)
		y += math.Cos(lat) * math.Sin(lon)
		z += math.Sin(lat)
		frp += d.FRP
		if d.Confidence == "h" {
			confidence = "h"
		}
		if first.IsZero() || d.At.Before(first) {
			first = d.At
		}
		satellites = append(satellites, d.Satellite)
	}
	slices.Sort(satellites)
	satellites = slices.Compact(satellites)
	latitude := math.Atan2(z, math.Hypot(x, y)) * 180 / math.Pi
	longitude := math.Atan2(y, x) * 180 / math.Pi
	area, best := "", math.Inf(1)
	for _, province := range p.provinces {
		km := distance(latitude, longitude, province.Lat, province.Lon)
		if km < best {
			area, best = province.Name, km
		}
	}
	severity := domain.SeverityWaspada
	switch {
	case len(keys) >= 20 || frp >= 100:
		severity = domain.SeverityAwas
	case len(keys) >= 6 || frp >= 30:
		severity = domain.SeveritySiaga
	}
	return domain.HazardEvent{HazardID: domain.HazardID(domain.SourceSatellite, ref), Source: domain.SourceSatellite,
		HazardType: domain.HazardWildfire, SourceRefID: ref, Severity: severity, AreaName: area,
		OccurredAt: first, Latitude: &latitude, Longitude: &longitude, Attributes: map[string]any{
			"detection_count": len(keys), "satellites": satellites, "max_confidence": confidence,
			"total_frp_mw": frp, "detection_keys": keys,
		}}
}

func LoadProvinces(data []byte) ([]province, error) {
	var provinces []province
	if err := json.Unmarshal(data, &provinces); err != nil {
		return nil, err
	}
	if len(provinces) == 0 {
		return nil, errors.New("province table is empty")
	}
	return provinces, nil
}
