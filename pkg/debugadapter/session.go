package debugadapter

import (
	"sync"
	"time"
)

// bufferedMessage is a single DAP message with an assigned sequence number.
type bufferedMessage struct {
	payload  []byte
	sequence int64
}

// session holds the state for a single debug session identified by an
// invocation ID. It contains two directional message buffers
// (client→worker and worker→client) and tracks liveness.
type session struct {
	mu sync.Mutex

	// Messages from the client destined for the worker.
	clientToWorker []bufferedMessage
	// Messages from the worker destined for the client.
	workerToClient []bufferedMessage

	// Monotonically increasing sequence counters per direction.
	clientToWorkerSeq int64
	workerToClientSeq int64

	// Closed and recreated when new messages are available.
	// Readers select on this channel; writers close and replace it.
	workerNotify chan struct{}
	clientNotify chan struct{}

	// Last time any activity occurred on this session.
	lastActivity time.Time

	// Set to true when the session should be torn down.
	ended bool
}

// newSession creates a new debug session.
func newSession() *session {
	return &session{
		workerNotify: make(chan struct{}),
		clientNotify: make(chan struct{}),
		lastActivity: time.Now(),
	}
}

// appendClientToWorker adds messages from the client to be delivered to
// the worker.
func (s *session) appendClientToWorker(messages [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	for _, msg := range messages {
		s.clientToWorkerSeq++
		s.clientToWorker = append(s.clientToWorker, bufferedMessage{
			payload:  msg,
			sequence: s.clientToWorkerSeq,
		})
	}
	s.lastActivity = time.Now()
	close(s.workerNotify)
	s.workerNotify = make(chan struct{})
}

// appendWorkerToClient adds messages from the worker to be delivered to
// the client.
func (s *session) appendWorkerToClient(messages [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	for _, msg := range messages {
		s.workerToClientSeq++
		s.workerToClient = append(s.workerToClient, bufferedMessage{
			payload:  msg,
			sequence: s.workerToClientSeq,
		})
	}
	s.lastActivity = time.Now()
	close(s.clientNotify)
	s.clientNotify = make(chan struct{})
}

// getClientToWorkerAfter returns all buffered messages with sequence
// numbers greater than lastSeq. Returns nil if none are available.
func (s *session) getClientToWorkerAfter(lastSeq int64) []bufferedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messagesAfter(s.clientToWorker, lastSeq)
}

// getWorkerToClientAfter returns all buffered messages with sequence
// numbers greater than lastSeq. Returns nil if none are available.
func (s *session) getWorkerToClientAfter(lastSeq int64) []bufferedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messagesAfter(s.workerToClient, lastSeq)
}

// workerWaitChan returns a channel that is closed when new messages for
// the worker are available or the session ends.
func (s *session) workerWaitChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workerNotify
}

// clientWaitChan returns a channel that is closed when new messages for
// the client are available or the session ends.
func (s *session) clientWaitChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientNotify
}

// messagesAfter returns messages with sequence > lastSeq. Must be
// called with s.mu held.
func (s *session) messagesAfter(buf []bufferedMessage, lastSeq int64) []bufferedMessage {
	for i, msg := range buf {
		if msg.sequence > lastSeq {
			return buf[i:]
		}
	}
	return nil
}

// end marks the session as ended and wakes all waiting goroutines.
func (s *session) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	close(s.workerNotify)
	close(s.clientNotify)
}

// isEnded returns whether the session has been ended.
func (s *session) isEnded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

// isInactive returns true if no activity has occurred for longer than
// the given timeout.
func (s *session) isInactive(timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.lastActivity) > timeout
}

// touch updates the last activity timestamp.
func (s *session) touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()
}
