package sharded

import (
	"context"
	"errors"
	"fmt"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func memberName(shard string, index int) string {
	return fmt.Sprintf("%s-%d", shard, index)
}

func (s shard) unreachableMembers(ctx context.Context) []string {
	done := make(chan string, len(s.members))
	for i, member := range s.members {
		go func() {
			missing := ""
			if member.Ping(ctx) != nil {
				missing = memberName(s.name, i)
			}
			done <- missing
		}()
	}
	var missing []string
	for range s.members {
		if name := <-done; name != "" {
			missing = append(missing, name)
		}
	}
	return missing
}

func (r *Repo) Locate(ctx context.Context, id string) ([]ports.Location, bool, error) {
	s := r.owner(id)
	type result struct {
		locations []ports.Location
		partial   bool
		err       error
	}
	results := fanout(ctx, []shard{*s}, func(ctx context.Context, s shard) result {
		out := result{locations: []ports.Location{}}
		h, err := s.unit.Get(ctx, id)
		if errors.Is(err, ports.ErrNotFound) || (err == nil && h.Retired) {
			out.err = ports.ErrNotFound
			return out
		}
		out.partial = err != nil || len(s.members) == 0
		for i, member := range s.members {
			loc, found, err := member.Placement(ctx, id, memberName(s.name, i))
			if err != nil || loc.Node == "" || (found != (h != nil && !h.Retired)) {
				out.partial = true
			}
			if err == nil && found && loc.Node != "" {
				out.locations = append(out.locations, loc)
			}
		}
		return out
	})
	out := results[0]
	return out.locations, out.partial, out.err
}
