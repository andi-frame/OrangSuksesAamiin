package obs

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// A tiny Prometheus registry (text exposition format 0.0.4). It keeps pkg/obs
// free of third-party dependencies: every service gets /metrics through
// Middleware without importing anything new.
//
// Metric names follow Prometheus conventions (snake_case, _total for counters,
// _seconds for durations). Label values are free text; label names are fixed
// when the metric is created.

// DefaultBuckets suit request latencies from 1 ms to 10 s.
var DefaultBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Registry holds metric families in registration order.
type Registry struct {
	mu       sync.Mutex
	families map[string]collector
	order    []string
}

type collector interface {
	write(w io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{families: map[string]collector{}} }

// DefaultRegistry is what /metrics serves.
var DefaultRegistry = NewRegistry()

// register returns the existing family of that name (so package-level metrics
// declared twice, e.g. in tests, are shared) or stores c.
func (r *Registry) register(name string, c collector) collector {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.families[name]; ok {
		return old
	}
	r.families[name] = c
	r.order = append(r.order, name)
	return c
}

// WriteText writes every family in the text exposition format.
func (r *Registry) WriteText(w io.Writer) {
	r.mu.Lock()
	cs := make([]collector, 0, len(r.order))
	for _, n := range r.order {
		cs = append(cs, r.families[n])
	}
	r.mu.Unlock()
	for _, c := range cs {
		c.write(w)
	}
}

// MetricsHandler serves the registry (plus Go runtime gauges).
func (r *Registry) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.WriteText(w)
	})
}

// ---------------------------------------------------------------- vectors ---

type vec struct {
	name, help, typ string
	labels          []string
	mu              sync.RWMutex
	series          map[string]*series
}

type series struct {
	values []string
	val    atomic.Uint64 // float64 bits (counter, gauge)
	// histogram
	hmu     sync.Mutex
	buckets []uint64
	sum     float64
	count   uint64
}

func newVec(name, help, typ string, labels []string) *vec {
	return &vec{name: name, help: help, typ: typ, labels: labels, series: map[string]*series{}}
}

func (v *vec) get(values []string, nb int) *series {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metric %s: %d label values for %d labels", v.name, len(values), len(v.labels)))
	}
	key := strings.Join(values, "\xff")
	v.mu.RLock()
	s, ok := v.series[key]
	v.mu.RUnlock()
	if ok {
		return s
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if s, ok = v.series[key]; ok {
		return s
	}
	s = &series{values: slices.Clone(values)}
	if nb > 0 {
		s.buckets = make([]uint64, nb)
	}
	v.series[key] = s
	return s
}

func (v *vec) sorted() []*series {
	v.mu.RLock()
	out := make([]*series, 0, len(v.series))
	for _, s := range v.series {
		out = append(out, s)
	}
	v.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return strings.Join(out[i].values, "\xff") < strings.Join(out[j].values, "\xff")
	})
	return out
}

func (v *vec) header(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", v.name, escHelp(v.help), v.name, v.typ)
}

func labelStr(names, values []string, extra ...string) string {
	if len(names) == 0 && len(extra) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	n := 0
	for i, k := range names {
		if n > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k + `="` + escLabel(values[i]) + `"`)
		n++
	}
	for i := 0; i+1 < len(extra); i += 2 {
		if n > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extra[i] + `="` + escLabel(extra[i+1]) + `"`)
		n++
	}
	b.WriteByte('}')
	return b.String()
}

func escLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func escHelp(s string) string { return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s) }

func fmtFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func addFloat(a *atomic.Uint64, d float64) {
	for {
		old := a.Load()
		if a.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+d)) {
			return
		}
	}
}

// ---------------------------------------------------------------- counter ---

// CounterVec is a monotonically increasing counter per label combination.
type CounterVec struct{ v *vec }

// NewCounter registers a counter in DefaultRegistry.
func NewCounter(name, help string, labels ...string) *CounterVec {
	return DefaultRegistry.NewCounter(name, help, labels...)
}

func (r *Registry) NewCounter(name, help string, labels ...string) *CounterVec {
	c := r.register(name, &CounterVec{newVec(name, help, "counter", labels)})
	return c.(*CounterVec)
}

func (c *CounterVec) Add(d float64, values ...string) {
	if d < 0 {
		return
	}
	addFloat(&c.v.get(values, 0).val, d)
}

func (c *CounterVec) Inc(values ...string) { c.Add(1, values...) }

// Value returns the current value (tests, /stats endpoints).
func (c *CounterVec) Value(values ...string) float64 {
	return math.Float64frombits(c.v.get(values, 0).val.Load())
}

func (c *CounterVec) write(w io.Writer) {
	c.v.header(w)
	for _, s := range c.v.sorted() {
		fmt.Fprintf(w, "%s%s %s\n", c.v.name, labelStr(c.v.labels, s.values), fmtFloat(math.Float64frombits(s.val.Load())))
	}
}

// ------------------------------------------------------------------ gauge ---

// GaugeVec is a value that goes up and down.
type GaugeVec struct{ v *vec }

func NewGauge(name, help string, labels ...string) *GaugeVec {
	return DefaultRegistry.NewGauge(name, help, labels...)
}

func (r *Registry) NewGauge(name, help string, labels ...string) *GaugeVec {
	g := r.register(name, &GaugeVec{newVec(name, help, "gauge", labels)})
	return g.(*GaugeVec)
}

func (g *GaugeVec) Set(x float64, values ...string) {
	g.v.get(values, 0).val.Store(math.Float64bits(x))
}

func (g *GaugeVec) Add(d float64, values ...string) { addFloat(&g.v.get(values, 0).val, d) }

func (g *GaugeVec) Value(values ...string) float64 {
	return math.Float64frombits(g.v.get(values, 0).val.Load())
}

func (g *GaugeVec) write(w io.Writer) {
	g.v.header(w)
	for _, s := range g.v.sorted() {
		fmt.Fprintf(w, "%s%s %s\n", g.v.name, labelStr(g.v.labels, s.values), fmtFloat(math.Float64frombits(s.val.Load())))
	}
}

// gaugeFunc reads its value when scraped.
type gaugeFunc struct {
	name, help string
	fn         func() float64
}

// NewGaugeFunc registers a gauge whose value is computed at scrape time.
func NewGaugeFunc(name, help string, fn func() float64) {
	DefaultRegistry.register(name, &gaugeFunc{name: name, help: help, fn: fn})
}

func (g *gaugeFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %s\n", g.name, escHelp(g.help), g.name, g.name, fmtFloat(g.fn()))
}

// -------------------------------------------------------------- histogram ---

// HistogramVec counts observations in cumulative buckets.
type HistogramVec struct {
	v       *vec
	bounds  []float64
	buckets int
}

func NewHistogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	return DefaultRegistry.NewHistogram(name, help, buckets, labels...)
}

func (r *Registry) NewHistogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	b := slices.Clone(buckets)
	sort.Float64s(b)
	h := r.register(name, &HistogramVec{v: newVec(name, help, "histogram", labels), bounds: b, buckets: len(b)})
	return h.(*HistogramVec)
}

func (h *HistogramVec) Observe(x float64, values ...string) {
	s := h.v.get(values, h.buckets)
	i := sort.SearchFloat64s(h.bounds, x) // first bound >= x
	s.hmu.Lock()
	if i < len(s.buckets) {
		s.buckets[i]++
	}
	s.sum += x
	s.count++
	s.hmu.Unlock()
}

// ObserveSince records the seconds elapsed since t.
func (h *HistogramVec) ObserveSince(t time.Time, values ...string) {
	h.Observe(time.Since(t).Seconds(), values...)
}

// Count returns the number of observations (tests).
func (h *HistogramVec) Count(values ...string) uint64 {
	s := h.v.get(values, h.buckets)
	s.hmu.Lock()
	defer s.hmu.Unlock()
	return s.count
}

func (h *HistogramVec) write(w io.Writer) {
	h.v.header(w)
	for _, s := range h.v.sorted() {
		s.hmu.Lock()
		counts, sum, count := slices.Clone(s.buckets), s.sum, s.count
		s.hmu.Unlock()
		var cum uint64
		for i, b := range h.bounds {
			cum += counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.v.name, labelStr(h.v.labels, s.values, "le", fmtFloat(b)), cum)
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.v.name, labelStr(h.v.labels, s.values, "le", "+Inf"), count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.v.name, labelStr(h.v.labels, s.values), fmtFloat(sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.v.name, labelStr(h.v.labels, s.values), count)
	}
}

// ---------------------------------------------------------- standard set ---

var (
	httpRequests = NewCounter("http_server_requests_total",
		"Inbound HTTP requests by route and status code.", "service", "method", "route", "code")
	httpDuration = NewHistogram("http_server_request_duration_seconds",
		"Inbound HTTP request latency.", DefaultBuckets, "service", "method", "route")
	httpInFlight = NewGauge("http_server_in_flight_requests",
		"Inbound HTTP requests being served.", "service")
	clientRequests = NewCounter("http_client_requests_total",
		"Outbound HTTP calls by target and status code (code=error: no response).", "service", "target", "code")
	clientDuration = NewHistogram("http_client_request_duration_seconds",
		"Outbound HTTP call latency.", DefaultBuckets, "service", "target")
	startTime = time.Now()
)

func init() {
	NewGaugeFunc("process_start_time_seconds", "Start time of the process since unix epoch in seconds.",
		func() float64 { return float64(startTime.UnixNano()) / 1e9 })
	NewGaugeFunc("go_goroutines", "Number of goroutines that currently exist.",
		func() float64 { return float64(runtime.NumGoroutine()) })
	NewGaugeFunc("go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.", func() float64 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return float64(m.HeapAlloc)
	})
}

// routeOf is the matched ServeMux pattern without its method ("/v1/hazards/{id}"),
// which keeps label cardinality bounded (never the raw path with ids in it).
func routeOf(r *http.Request) string {
	p := r.Pattern
	if p == "" {
		return "unmatched"
	}
	if _, after, ok := strings.Cut(p, " "); ok {
		p = after
	}
	return p
}
