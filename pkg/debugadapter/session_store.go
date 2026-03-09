package debugadapter

import (
	"sync"
	"time"
)

// SessionStore manages debug sessions keyed by invocation ID. It
// provides thread-safe access to sessions and runs a background reaper
// that removes inactive sessions.
type SessionStore struct {
	mu       sync.Mutex
	sessions map[string]*session

	inactivityTimeout time.Duration
}

// NewSessionStore creates a new session store. The inactivityTimeout
// controls how long a session can go without any message activity
// before being automatically reaped.
func NewSessionStore(inactivityTimeout time.Duration) *SessionStore {
	return &SessionStore{
		sessions:          make(map[string]*session),
		inactivityTimeout: inactivityTimeout,
	}
}

// Register creates a new session for the given invocation ID. Returns
// false if a session already exists for this ID.
func (ss *SessionStore) Register(invocationID string) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if _, exists := ss.sessions[invocationID]; exists {
		return false
	}
	ss.sessions[invocationID] = newSession()
	return true
}

// Get returns the session for the given invocation ID, or nil if no
// session exists.
func (ss *SessionStore) Get(invocationID string) *session {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.sessions[invocationID]
}

// Remove ends and removes a session from the store.
func (ss *SessionStore) Remove(invocationID string) {
	ss.mu.Lock()
	s := ss.sessions[invocationID]
	delete(ss.sessions, invocationID)
	ss.mu.Unlock()
	if s != nil {
		s.end()
	}
}

// ReapInactive removes all sessions that have been inactive for longer
// than the configured timeout. This is intended to be called
// periodically from a background goroutine.
func (ss *SessionStore) ReapInactive() {
	ss.mu.Lock()
	var toRemove []string
	for id, s := range ss.sessions {
		if s.isInactive(ss.inactivityTimeout) {
			toRemove = append(toRemove, id)
		}
	}
	for _, id := range toRemove {
		s := ss.sessions[id]
		delete(ss.sessions, id)
		s.end()
	}
	ss.mu.Unlock()
}
