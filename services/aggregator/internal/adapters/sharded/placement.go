package sharded

import (
	"context"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// Placement metrics (P1 criteria 1, 2 and 5): which node holds each storage unit, in which role, and how many
// active HazardEvents it holds. Every Aggregator instance reports the same view, so dashboards use max().
var (
	unitUp = obs.NewGauge("aggregator_store_unit_up",
		"1 while the storage unit (shard member) answers, by shard, unit, node and role (primary, replica).",
		"shard", "unit", "node", "role")
	unitRows = obs.NewGauge("aggregator_store_unit_rows",
		"Active HazardEvents held by the storage unit (a replica holds the same rows as its primary).",
		"shard", "unit", "node", "role")
)

// ExportPlacement refreshes the placement metrics until ctx ends. A unit that stops answering, or moves to another
// node or role, has its previous series set to 0, so a dashboard shows the change instead of a stale line.
func (r *Repo) ExportPlacement(ctx context.Context) {
	type key struct{ shard, unit, node, role string }
	previous := map[key]bool{}
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		current := map[key]float64{}
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, s := range r.shards {
			for i, member := range s.members {
				wg.Add(1)
				go func() {
					defer wg.Done()
					qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
					node, replica, rows, err := member.UnitStats(qctx)
					if err != nil || node == "" {
						return
					}
					role := "primary"
					if replica {
						role = "replica"
					}
					mu.Lock()
					current[key{s.name, memberName(s.name, i), node, role}] = float64(rows)
					mu.Unlock()
				}()
			}
		}
		wg.Wait()
		for k := range previous {
			if _, ok := current[k]; !ok {
				unitUp.Set(0, k.shard, k.unit, k.node, k.role)
				unitRows.Set(0, k.shard, k.unit, k.node, k.role)
			}
		}
		previous = map[key]bool{}
		for k, rows := range current {
			unitUp.Set(1, k.shard, k.unit, k.node, k.role)
			unitRows.Set(rows, k.shard, k.unit, k.node, k.role)
			previous[k] = true
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
