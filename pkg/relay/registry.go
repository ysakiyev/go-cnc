package relay

import (
	"errors"
	"sync"

	"github.com/google/uuid"

	"go-cnc2/pkg/authn"
	pb "go-cnc2/proto/pb"
)

type halfSession struct {
	stream pb.RelayService_TunnelServer
}

type session struct {
	mu     sync.Mutex
	client *halfSession
	agent  *halfSession
	paired chan struct{} // closed when both halves are present
	done   chan struct{} // closed when copy goroutines finish
}

type Registry struct {
	mu       sync.Mutex
	sessions map[uuid.UUID]*session
}

func NewRegistry() *Registry {
	return &Registry{sessions: make(map[uuid.UUID]*session)}
}

// Attach registers one half of a tunnel. Returns (sess, isSecond, error).
// isSecond is true for the caller that completes the pair — that caller is
// responsible for starting the copy goroutines.
func (r *Registry) Attach(sessionID uuid.UUID, role authn.Role, stream pb.RelayService_TunnelServer) (*session, bool, error) {
	r.mu.Lock()
	sess, ok := r.sessions[sessionID]
	if !ok {
		sess = &session{
			paired: make(chan struct{}),
			done:   make(chan struct{}),
		}
		r.sessions[sessionID] = sess
	}
	r.mu.Unlock()

	sess.mu.Lock()
	defer sess.mu.Unlock()

	half := &halfSession{stream: stream}

	switch role {
	case authn.RoleClient:
		if sess.client != nil {
			return nil, false, errors.New("relay: client slot already filled")
		}
		sess.client = half
	case authn.RoleAgent:
		if sess.agent != nil {
			return nil, false, errors.New("relay: agent slot already filled")
		}
		sess.agent = half
	default:
		return nil, false, errors.New("relay: unknown role")
	}

	if sess.client != nil && sess.agent != nil {
		close(sess.paired)
		return sess, true, nil
	}
	return sess, false, nil
}

func (r *Registry) Remove(sessionID uuid.UUID) {
	r.mu.Lock()
	delete(r.sessions, sessionID)
	r.mu.Unlock()
}
