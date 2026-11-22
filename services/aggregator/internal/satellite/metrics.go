package satellite

import "github.com/andi-frame/orangsuksesaamiin/pkg/obs"

// Prometheus series of the Feed Satelit consumer (P4 criterion 4: how far behind the system is during a burst must
// be visible on the dashboard). Only the elected leader consumes the feed, so dashboards use max() over pods.
var (
	metricHead = obs.NewGauge("satellite_feed_head_offset",
		"Latest offset the feed reported in /v1/session (polled every 3 s while connected).")
	metricApplied = obs.NewGauge("satellite_applied_offset",
		"Offset up to which every change is stored as HazardEvent (the durable resume position).")
	metricLag = obs.NewGauge("satellite_lag_offsets",
		"Feed head minus applied offset: events received by the feed but not yet stored.")
	metricLate = obs.NewGauge("satellite_late_detections",
		"Detections that arrived behind the event-time watermark (counted, kept, not clustered).")
	metricSingletons = obs.NewGauge("satellite_singleton_detections",
		"Accepted detections that are not connected to any other detection (not a fire, but reported).")
	metricActiveFires = obs.NewGauge("satellite_active_fires",
		"Fires (connected groups of >= 2 detections) currently held by the consumer.")
	metricEvents = obs.NewCounter("satellite_events_total",
		"Feed events consumed, by SSE event type (detection, update, retract, heartbeat).", "kind")
	metricRejected = obs.NewCounter("satellite_events_rejected_total",
		"Feed events whose content could not be applied (logged with the raw text and skipped, never blocking).")
	metricDisconnects = obs.NewCounter("satellite_stream_disconnects_total",
		"Stream connections that ended (network error, idle timeout, offset gap, HTTP error).")
	metricStoreLatency = obs.NewHistogram("satellite_store_latency_seconds",
		"From receiving the feed event that changed a fire to the HazardEvent being stored.",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300})
)
