package pvmbg

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

var levels = []string{"Normal", "Waspada", "Siaga", "Awas"}

type volcano struct {
	id    string
	level int // index into levels, drifts over time
}

// Generator produces seed data and a steady stream of reports.
type Generator struct {
	cfg   Config
	store *Store
	state *State
	log   *slog.Logger

	mu       sync.Mutex
	rng      *rand.Rand
	runID    string
	seq      int
	volcanos []*volcano
}

// NewGenerator builds a generator. The volcano ids match the static reference
// table owned by BNPB (services/aggregator/config/volcanoes.json).
func NewGenerator(cfg Config, store *Store, state *State, log *slog.Logger) *Generator {
	var b [3]byte
	_, _ = crand.Read(b[:])
	return &Generator{
		cfg: cfg, store: store, state: state, log: log,
		rng:   rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0x50564d42)),
		runID: hex.EncodeToString(b[:]),
		volcanos: []*volcano{
			{"merapi", 2}, {"semeru", 1}, {"anak-krakatau", 1}, {"sinabung", 2},
			{"ruang", 0}, {"lewotobi", 3}, {"prikitiw", 1},
		},
	}
}

func (g *Generator) uniform(lo, hi float64) float64 { return lo + g.rng.Float64()*(hi-lo) }

func round(v float64, d int) float64 {
	p := math.Pow10(d)
	return math.Round(v*p) / p
}

func levelIndex(name string) int {
	for i, l := range levels {
		if l == name {
			return i
		}
	}
	return -1
}

// newReport builds one report at time at. volcanoID/level override the random
// choice when non-empty. confidence is attached only while schema v2 is on.
func (g *Generator) newReport(at time.Time, volcanoID, level string, withConfidence bool, extra map[string]any) Report {
	g.mu.Lock()
	defer g.mu.Unlock()
	var v *volcano
	if volcanoID != "" {
		for _, c := range g.volcanos {
			if c.id == volcanoID {
				v = c
			}
		}
	}
	if v == nil {
		v = g.volcanos[g.rng.IntN(len(g.volcanos))]
	}
	if li := levelIndex(level); li >= 0 {
		v.level = li
	} else if g.rng.Float64() < 0.10 { // occasional escalation / de-escalation
		if d := g.rng.IntN(2)*2 - 1; v.level+d >= 0 && v.level+d < len(levels) {
			v.level += d
		}
	}
	var count int
	var ash float64
	switch v.level {
	case 0:
		count, ash = g.rng.IntN(2), g.uniform(0, 200)
	case 1:
		count, ash = g.rng.IntN(6), g.uniform(100, 1500)
	case 2:
		count, ash = 3+g.rng.IntN(13), g.uniform(800, 4000)
	default:
		count, ash = 10+g.rng.IntN(31), g.uniform(3000, 9000)
	}
	g.seq++
	r := Report{
		ReportID:         fmt.Sprintf("VR-%s-%04d", g.runID, g.seq),
		VolcanoID:        v.id,
		AlertLevel:       levels[v.level],
		EruptionCount24h: count,
		AshColumnHeightM: round(ash, 1),
		ReportedAt:       at.UTC(),
		Extra:            extra,
	}
	if withConfidence {
		c := round(g.uniform(0.5, 1.0), 2)
		r.Confidence = &c
	}
	return r
}

// Seed creates the historical records (>= 20 per the contract). The seed never
// carries confidence_level: that field only appears after the schema switch.
func (g *Generator) Seed(now time.Time) {
	// Each volcano keeps its base alert level throughout the seed (no random
	// drift), so all four levels (Normal/Waspada/Siaga/Awas) are always present.
	base := make([]string, len(g.volcanos))
	for i, v := range g.volcanos {
		base[i] = levels[v.level]
	}
	for i := 0; i < g.cfg.SeedCount; i++ {
		at := now.Add(-time.Duration(g.cfg.SeedCount-i) * 2 * time.Hour)
		vi := i % len(g.volcanos)
		g.store.Add(g.newReport(at, g.volcanos[vi].id, base[vi], false, nil))
	}
	g.log.Info("seeded", "reports", g.cfg.SeedCount)
}

// Generate creates n reports right now. override entries are merged into the wire
// format of these reports on top of the persistent extra fields (a nil value
// removes a field), which lets an operator simulate unsupported schema changes.
func (g *Generator) Generate(n int, volcanoID, level string, override map[string]any) []Wire {
	withConf := g.state.Confidence()
	out := make([]Wire, 0, n)
	for i := 0; i < n; i++ {
		extra := g.state.Extra()
		for k, v := range override {
			extra[k] = v
		}
		r := g.newReport(time.Now(), volcanoID, level, withConf, extra)
		g.store.Add(r)
		out = append(out, r.render(withConf))
		g.log.Info("report_generated", "report_id", r.ReportID, "volcano_id", r.VolcanoID,
			"alert_level", r.AlertLevel, "has_confidence_level", r.Confidence != nil, "extra_fields", len(extra))
	}
	return out
}

// Run emits one report per cfg.ReportInterval until ctx is cancelled.
func (g *Generator) Run(ctx context.Context) {
	if g.cfg.ReportInterval <= 0 {
		return
	}
	t := time.NewTicker(g.cfg.ReportInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.Generate(1, "", "", nil)
		}
	}
}
