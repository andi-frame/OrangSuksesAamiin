package bmkg

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

type region struct {
	name     string
	lat, lon float64
	zones    []string
}

var regions = []region{
	{"Selat Sunda", -6.10, 105.80, []string{"Anyer", "Carita", "Labuan", "Kalianda"}},
	{"Laut Banda", -6.20, 127.50, []string{"Banda Neira", "Ambon", "Tual"}},
	{"Sumatera Barat", -0.95, 100.35, []string{"Padang", "Pariaman", "Mentawai"}},
	{"Jawa Barat Selatan", -7.80, 107.50, []string{"Pangandaran", "Garut Selatan", "Cianjur Selatan"}},
	{"Nusa Tenggara Timur", -9.80, 122.00, []string{"Sumba", "Kupang", "Rote"}},
	{"Maluku Utara", 1.50, 127.80, []string{"Ternate", "Tidore", "Morotai"}},
	{"Sulawesi Tengah", -1.00, 120.00, []string{"Palu", "Donggala", "Parigi"}},
	{"Aceh Barat", 4.30, 95.90, []string{"Meulaboh", "Calang", "Simeulue"}},
	{"Bengkulu", -4.20, 102.00, []string{"Bengkulu Kota", "Seluma", "Mukomuko"}},
	{"Papua Barat", -1.50, 132.50, []string{"Manokwari", "Sorong", "Teluk Bintuni"}},
}

// Generator produces seed data and a steady stream of new events.
type Generator struct {
	cfg   Config
	store *Store
	log   *slog.Logger

	mu    sync.Mutex
	rng   *rand.Rand
	runID string
	seq   int
	wg    sync.WaitGroup
}

// NewGenerator builds a generator. The per-run id embedded in every event id
// keeps ids unique across mock restarts (a restart re-seeds the data set).
func NewGenerator(cfg Config, store *Store, log *slog.Logger) *Generator {
	var b [3]byte
	_, _ = crand.Read(b[:])
	return &Generator{
		cfg:   cfg,
		store: store,
		log:   log,
		rng:   rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0x424d4b47)),
		runID: hex.EncodeToString(b[:]),
	}
}

func (g *Generator) nextID() string {
	g.seq++
	return fmt.Sprintf("EQ-%s-%04d", g.runID, g.seq)
}

func (g *Generator) uniform(lo, hi float64) float64 { return lo + g.rng.Float64()*(hi-lo) }

func round(v float64, decimals int) float64 {
	p := math.Pow10(decimals)
	return math.Round(v*p) / p
}

// spec describes an event to create; zero values are randomised.
type spec struct {
	magnitude float64
	tsunami   *bool
	at        time.Time
}

func (g *Generator) newEvent(s spec) SeismicEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	mag := s.magnitude
	if mag == 0 {
		switch r := g.rng.Float64(); {
		case r < 0.70:
			mag = g.uniform(3.0, 4.9)
		case r < 0.90:
			mag = g.uniform(5.0, 6.4)
		default:
			mag = g.uniform(6.5, 7.8)
		}
	}
	depth := g.uniform(5, 250)
	tsunami := false
	if s.tsunami != nil {
		tsunami = *s.tsunami
	} else if mag >= 6.0 && g.rng.Float64() < 0.8 {
		tsunami = true
	}
	if tsunami {
		depth = g.uniform(8, 50) // shallow, as tsunamigenic events are
	}
	rg := regions[g.rng.IntN(len(regions))]
	return SeismicEvent{
		EventID:          g.nextID(),
		Magnitude:        round(mag, 1),
		DepthKm:          round(depth, 1),
		EpicenterLat:     round(rg.lat+g.uniform(-0.6, 0.6), 4),
		EpicenterLon:     round(rg.lon+g.uniform(-0.6, 0.6), 4),
		RegionName:       rg.name,
		OccurredAt:       s.at.UTC(),
		PotentialTsunami: tsunami,
	}
}

func threatFor(mag float64) string {
	switch {
	case mag >= 7.4:
		return "Awas"
	case mag >= 6.9:
		return "Siaga"
	default:
		return "Waspada"
	}
}

func (g *Generator) newWarning(e SeismicEvent, issuedAt time.Time) TsunamiWarning {
	g.mu.Lock()
	defer g.mu.Unlock()
	var zones []string
	for _, rg := range regions {
		if rg.name == e.RegionName {
			zones = append(zones, rg.zones...)
		}
	}
	return TsunamiWarning{
		WarningID:        "TW-" + e.EventID[3:],
		RelatedEventID:   e.EventID,
		ThreatLevel:      threatFor(e.Magnitude),
		AffectedZones:    zones,
		EstimatedArrival: issuedAt.Add(time.Duration(10+g.rng.IntN(30)) * time.Minute).UTC(),
		IssuedAt:         issuedAt.UTC(),
	}
}

// Seed creates the historical records (>= 20 per the contract). Every 5th
// event is a tsunami-capable one with a matching warning, covering all three
// threat levels.
func (g *Generator) Seed(now time.Time) {
	forced := []float64{7.6, 7.0, 6.6} // tsunami warnings: Awas, Siaga, Waspada
	// deterministic non-tsunami magnitudes so that every severity branch of the
	// mapping (NORMAL < 5.0, WASPADA 5.0-6.4, SIAGA >= 6.5) is present in the seed
	plain := []float64{3.4, 4.2, 5.3, 5.9, 6.7, 4.8, 6.1, 3.9}
	fi, pi := 0, 0
	for i := 0; i < g.cfg.SeedCount; i++ {
		at := now.Add(-time.Duration(g.cfg.SeedCount-i) * 90 * time.Minute)
		s := spec{at: at}
		if i%5 == 4 {
			t := true
			s.tsunami = &t
			s.magnitude = forced[fi%len(forced)]
			fi++
		} else {
			f := false
			s.tsunami = &f
			s.magnitude = plain[pi%len(plain)]
			pi++
		}
		e := g.newEvent(s)
		g.store.AddEvent(e)
		if e.PotentialTsunami {
			g.store.AddWarning(g.newWarning(e, at.Add(2*time.Minute)))
		}
	}
	g.log.Info("seeded", "events", g.cfg.SeedCount)
}

// Generate creates n events right now (used by the ticker and /admin/generate).
// A tsunami-capable event gets its warning issued a few seconds later, so the
// aggregator has to correlate a warning that arrives after its event.
func (g *Generator) Generate(n int, magnitude float64, tsunami *bool) []SeismicEvent {
	out := make([]SeismicEvent, 0, n)
	for i := 0; i < n; i++ {
		e := g.newEvent(spec{at: time.Now(), magnitude: magnitude, tsunami: tsunami})
		g.store.AddEvent(e)
		out = append(out, e)
		g.log.Info("event_generated", "event_id", e.EventID, "magnitude", e.Magnitude, "potential_tsunami", e.PotentialTsunami)
		if e.PotentialTsunami {
			g.scheduleWarning(e)
		}
	}
	return out
}

func (g *Generator) scheduleWarning(e SeismicEvent) {
	g.mu.Lock()
	d := g.cfg.WarningDelayMin
	if span := g.cfg.WarningDelayMax - g.cfg.WarningDelayMin; span > 0 {
		d += time.Duration(g.rng.Int64N(int64(span)))
	}
	g.mu.Unlock()
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		time.Sleep(d)
		w := g.newWarning(e, time.Now())
		g.store.AddWarning(w)
		g.log.Info("warning_issued", "warning_id", w.WarningID, "related_event_id", w.RelatedEventID, "threat_level", w.ThreatLevel)
	}()
}

// Run emits one event per cfg.EventInterval until ctx is cancelled.
func (g *Generator) Run(ctx context.Context) {
	if g.cfg.EventInterval <= 0 {
		return
	}
	t := time.NewTicker(g.cfg.EventInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.Generate(1, 0, nil)
		}
	}
}
