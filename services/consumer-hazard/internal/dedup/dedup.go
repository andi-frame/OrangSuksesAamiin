package dedup

import "sync"

// Store remembers processed event_id values (at-least-once delivery).
type Store struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func New() *Store {
	return &Store{seen: map[string]struct{}{}}
}

// Mark returns false if event_id was already processed.
func (s *Store) Mark(eventID string) (firstTime bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[eventID]; ok {
		return false
	}
	s.seen[eventID] = struct{}{}
	return true
}

func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}
