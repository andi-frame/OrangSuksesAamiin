package ports

import "context"

// SatelliteStore persists the checkpoint and pending changes as one fenced
// document. The runner saves it before applying any change to a hazard shard.
type SatelliteStore interface {
	LoadSatellite(context.Context, string) ([]byte, error)
	StoreSatellite(context.Context, string, []byte) error
}

// HazardDurability verifies a visible version after an ambiguous commit.
type HazardDurability interface {
	ConfirmHazard(ctx context.Context, hazardID string, minVersion int) error
}
