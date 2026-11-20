#!/usr/bin/env python3
"""Generates argus-overview.json (the one metrics dashboard of M2 "Observabilitas").
Run after editing: python3 deploy/grafana/gen-dashboard.py"""
import json, os

pid = 0
def panel(title, targets, kind="timeseries", w=8, h=8, unit=None, desc="", **extra):
    global pid
    pid += 1
    p = {"id": pid, "type": kind, "title": title, "description": desc,
         "datasource": {"type": "prometheus", "uid": "prometheus"},
         "gridPos": {"w": w, "h": h},
         "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": {"type": "prometheus", "uid": "prometheus"}}
                     for i, (e, l) in enumerate(targets)],
         "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if kind == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never"}
        p["options"] = {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                        "tooltip": {"mode": "multi", "sort": "desc"}}
    p.update(extra)
    return p

def row(title):
    global pid
    pid += 1
    return {"id": pid, "type": "row", "title": title, "collapsed": False, "gridPos": {"w": 24, "h": 1}, "panels": []}

RATE = "[1m]"
panels = [
    row("Setiap service: throughput, error rate, latensi (kriteria Observabilitas 1)"),
    panel("Throughput per service (req/s)", [('sum by (app) (rate(http_server_requests_total' + RATE + '))', "{{app}}")], unit="reqps",
          desc="Permintaan masuk per detik per service (semua pod)."),
    panel("Error rate per service (5xx)", [('argus:http_error_ratio:rate1m', "{{app}}")], unit="percentunit",
          desc="Proporsi respons 5xx. 429 (penolakan terkontrol) tidak dihitung sebagai error."),
    panel("Latensi p95 per service", [('histogram_quantile(0.95, sum by (app, le) (rate(http_server_request_duration_seconds_bucket' + RATE + ')))', "{{app}}")], unit="s"),
    panel("Latensi p50 / p99 per route (client-api)", [
        ('histogram_quantile(0.50, sum by (route, le) (rate(http_server_request_duration_seconds_bucket{app="client-api"}' + RATE + ')))', "p50 {{route}}"),
        ('histogram_quantile(0.99, sum by (route, le) (rate(http_server_request_duration_seconds_bucket{app="client-api"}' + RATE + ')))', "p99 {{route}}")], unit="s"),
    panel("Status code per service", [('sum by (app, code) (rate(http_server_requests_total' + RATE + '))', "{{app}} {{code}}")], unit="reqps"),
    panel("Panggilan keluar: latensi p95 per tujuan", [('histogram_quantile(0.95, sum by (app, target, le) (rate(http_client_request_duration_seconds_bucket' + RATE + ')))', "{{app}} -> {{target}}")], unit="s"),
    panel("Panggilan keluar gagal per tujuan", [('sum by (app, target, code) (rate(http_client_requests_total{code=~"5..|error|4.."}' + RATE + '))', "{{app}} -> {{target}} {{code}}")], unit="reqps"),
    panel("Pod hidup (target scrape)", [('sum by (app) (up{job="argus"})', "{{app}}")], desc="Jumlah pod per service yang menjawab /metrics. Turun = pod mati / node hilang."),
    panel("Pod per node", [('count by (node) (up{job="argus"} == 1)', "{{node}}")]),

    row("Ingest: sumber, leader, outbox"),
    panel("Status sumber (1 up, 0.5 degraded, 0 down)", [('max by (source) (aggregator_source_state)', "{{source}}")], kind="stat", w=8, h=6,
          options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
          fieldConfig={"defaults": {"min": 0, "max": 1, "thresholds": {"mode": "absolute", "steps": [
              {"color": "red", "value": None}, {"color": "orange", "value": 0.5}, {"color": "green", "value": 1}]}}, "overrides": []}),
    panel("Circuit breaker terbuka", [('max by (source) (aggregator_source_breaker_open)', "{{source}}")], w=8, h=6),
    panel("Data basi sejak (detik)", [('max by (source) (aggregator_source_stale_seconds)', "{{source}}")], unit="s", w=8, h=6),
    panel("Siklus poll per hasil", [('sum by (source, result) (rate(aggregator_poll_cycles_total' + RATE + '))', "{{source}} {{result}}")], unit="ops"),
    panel("Leader & term (fencing token)", [('max by (pod) (aggregator_is_leader)', "leader {{pod}}"), ('max(aggregator_leader_term)', "term")]),
    panel("Outbox -> broker p95 dan kegagalan", [
        ('histogram_quantile(0.95, sum by (le) (rate(aggregator_outbox_delivery_seconds_bucket' + RATE + ')))', "p95 outbox->broker"),
        ('sum(rate(aggregator_outbox_publish_failures_total' + RATE + '))', "publish gagal/s")], unit="s"),
    panel("Pesan tak terproses (DLQ)", [('max by (source, status) (aggregator_unprocessed_messages)', "{{source}} {{status}}")]),
    panel("HazardEvent tersimpan per sumber", [('sum by (source, outcome) (rate(aggregator_hazards_stored_total' + RATE + '))', "{{source}} {{outcome}}")], unit="ops"),
    panel("Pembacaan parsial (unit penyimpanan tak terjangkau)", [('sum by (endpoint) (rate(aggregator_partial_reads_total' + RATE + ')) or vector(0)', "{{endpoint}}")], unit="reqps"),

    row("P5 Sensor Pantai: jalur peringatan Tim Lapangan (SLO 99 % <= 5 s)"),
    panel("SLO: peringatan kritis <= 5 s (rentang dashboard)", [
        ('sum(increase(consumer_critical_alert_seconds_bucket{le="5"}[$__range])) / clamp_min(sum(increase(consumer_critical_alert_seconds_count[$__range])), 1e-9)', "within 5 s")],
        kind="stat", w=6, h=6, unit="percentunit",
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
        fieldConfig={"defaults": {"unit": "percentunit", "decimals": 2, "thresholds": {"mode": "absolute", "steps": [
            {"color": "red", "value": None}, {"color": "green", "value": 0.99}]}}, "overrides": []}),
    panel("Error budget terpakai (target 99 %)", [
        ('(1 - (sum(increase(consumer_critical_alert_seconds_bucket{le="5"}[$__range])) / clamp_min(sum(increase(consumer_critical_alert_seconds_count[$__range])), 1e-9))) / 0.01', "budget terpakai"),
        ],
        kind="gauge", w=6, h=6, unit="percentunit",
        fieldConfig={"defaults": {"unit": "percentunit", "min": 0, "max": 1, "thresholds": {"mode": "absolute", "steps": [
            {"color": "green", "value": None}, {"color": "orange", "value": 0.5}, {"color": "red", "value": 1}]}}, "overrides": []}),
    panel("Kritis: dari sensor vs sampai notifier", [
        ('sum by (outcome) (increase(sensor_pantai_critical_total[$__range]))', "sensor kritis {{outcome}}"),
        ('sum(increase(consumer_critical_alerts_total{consumer="notifier"}[$__range]))', "sampai notifier")], kind="stat", w=6, h=6,
        options={"reduceOptions": {"calcs": ["lastNotNull"]}}),
    panel("Latensi peringatan kritis p50/p95", [
        ('histogram_quantile(0.95, sum by (le) (rate(consumer_critical_alert_seconds_bucket[5m])))', "p95 simpan->notifier"),
        ('histogram_quantile(0.95, sum by (le) (rate(sensor_pantai_critical_ingest_seconds_bucket[5m])))', "p95 fetch->simpan")], unit="s", w=6, h=6),
    panel("Permintaan ke Sensor Pantai (tidak boleh melonjak)", [('sum by (code) (rate(http_client_requests_total{target="sensor-pantai"}' + RATE + '))', "{{code}}")], unit="reqps",
          desc="Saat sensor tidak sehat, breaker membatasi panggilan menjadi 1 probe per SENSOR_BREAKER_OPEN_SEC."),
    panel("Perilaku menyimpang sensor terdeteksi", [('sum by (kind) (increase(sensor_pantai_anomalies_total[1m]))', "{{kind}}")]),
    panel("Observasi disisihkan per alasan", [('sum by (reason) (increase(sensor_pantai_observations_rejected_total[1m]))', "{{reason}}")]),
    panel("generated_at membeku (detik)", [('max(sensor_pantai_generated_at_frozen_seconds)', "frozen")], unit="s"),
    panel("Observasi sensor tersimpan per severity", [('sum by (severity) (rate(sensor_pantai_observations_stored_total' + RATE + '))', "{{severity}}")], unit="ops"),

    row("Client-Facing API: prioritas beban & laporan lapangan (P3 kriteria 6)"),
    panel("Admisi per peran (shed = 429 terkontrol)", [('sum by (role, outcome) (rate(client_api_admission_total' + RATE + '))', "{{role}} {{outcome}}")], unit="reqps"),
    panel("Laporan lapangan per hasil", [('sum by (outcome) (rate(client_api_field_reports_total' + RATE + '))', "API {{outcome}}"),
                                         ('sum by (outcome) (rate(field_reports_processed_total' + RATE + '))', "processor {{outcome}}")], unit="ops"),
    panel("Laporan lapangan: 202 dan end-to-end", [
        ('histogram_quantile(0.95, sum by (le) (rate(client_api_field_report_ack_seconds_bucket' + RATE + ')))', "p95 ack broker (sebelum 202)"),
        ('histogram_quantile(0.95, sum by (le) (rate(field_reports_end_to_end_seconds_bucket[5m])))', "p95 202 -> diproses")], unit="s"),

    row("Consumer (broker)"),
    panel("Event per consumer", [('sum by (consumer, outcome) (rate(consumer_events_total' + RATE + '))', "{{consumer}} {{outcome}}")], unit="ops"),
    panel("Latensi simpan -> consumer p95", [('histogram_quantile(0.95, sum by (consumer, le) (rate(consumer_delivery_seconds_bucket' + RATE + ')))', "{{consumer}}")], unit="s"),
    panel("Span trace diekspor / dibuang", [('sum by (service) (rate(obs_trace_spans_exported_total' + RATE + '))', "ekspor {{service}}"),
                                            ('sum by (service) (rate(obs_trace_spans_dropped_total' + RATE + '))', "buang {{service}}")], unit="ops"),
]

# simple flow layout
x = y = 0; rowh = 0
for p in panels:
    g = p["gridPos"]
    if p["type"] == "row":
        if x: y += rowh; x = 0; rowh = 0
        g.update(x=0, y=y); y += 1; continue
    if x + g["w"] > 24: x = 0; y += rowh; rowh = 0
    g.update(x=x, y=y); x += g["w"]; rowh = max(rowh, g["h"])

dash = {"uid": "argus-overview", "title": "Argus / Observabilitas", "tags": ["argus", "m2"], "timezone": "browser",
        "schemaVersion": 39, "version": 1, "refresh": "5s", "time": {"from": "now-30m", "to": "now"},
        "description": "Satu dashboard metrik untuk setiap service (M2). Trace: Explore -> Jaeger, cari trace_id dari log atau header traceparent.",
        "links": [{"title": "Jaeger (trace)", "url": "/explore?left=%7B%22datasource%22:%22jaeger%22%7D", "type": "link"}],
        "panels": panels}
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "argus-overview.json")
with open(out, "w") as f:
    json.dump(dash, f, indent=1)
print("wrote", out, len(panels), "panels")
