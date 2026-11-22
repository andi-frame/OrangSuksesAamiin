#!/usr/bin/env bash
# Summarize a named recording window. Use BURST_END_OFFSET and BURST_END_AT
# from the practice/exam feed to measure time from burst end to checkpoint.
. "$(dirname "${BASH_SOURCE[0]}")/k8s-lib.sh"
mkdir -p "$ROOT/docs/evidence/m2"
raw=$(mktemp)
trap 'rm -f "$raw"; cleanup_pf' EXIT
for pod in $(kn get pods -l app=aggregator -o name); do
  for previous in '' --previous; do
    kn logs "$pod" --since="${SINCE:-10m}" $previous 2>/dev/null |
      jqx -Rc 'fromjson? | select(.msg == "satellite_checkpoint" or .msg == "satellite_hazard_applied" or .msg == "satellite_stream_disconnected")' >> "$raw" || true
  done
done

{
  step "Satellite lag and receive-to-store latency"
  jqx -es '
    map(select(.msg=="satellite_hazard_applied") | .store_latency_ms) | sort as $latencies |
    {hazard_mutations:($latencies|length),
     p95_store_latency_ms:(if ($latencies|length)>0 then
       $latencies[((($latencies|length)*0.95|ceil)-1)] else null end)}' < "$raw"
  jqx -es --arg offset "${BURST_END_OFFSET:-}" --arg burst_end "${BURST_END_AT:-}" '
    def epoch: sub("\\.[0-9]+Z$";"Z") | fromdateiso8601;
    map(select(.msg=="satellite_checkpoint")) | sort_by(.ts) as $checkpoints |
    {checkpoints:($checkpoints|length), last:$checkpoints[-1],
     peak_lag_offsets:($checkpoints|map(.lag_offsets)|max),
     catchup_seconds:(if $offset != "" and $burst_end != "" then
       ([$checkpoints[] | select((.ts|epoch)>=($burst_end|epoch) and .applied_offset>=($offset|tonumber))] |
         if length>0 then (.[0].ts|epoch)-($burst_end|epoch) else null end)
       else null end)} | select(.checkpoints > 0)' < "$raw"
  step "Canonical wildfire checksum"
  forward aggregator 18083 8083
  audit '/internal/audit/checksum?source=SATELIT' | jqx -e 'select(.checksum | test("^[0-9a-f]{64}$"))'
} | tee "$ROOT/docs/evidence/m2/p4-feed-summary.log"
