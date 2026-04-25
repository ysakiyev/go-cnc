package relay

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go-cnc2/pkg/authn"
	pb "go-cnc2/proto/pb"
)

const handshakeTimeout = 10 * time.Second

type Service struct {
	registry *Registry
	keys     map[byte][]byte
}

func NewService(keys map[byte][]byte) *Service {
	return &Service{
		registry: NewRegistry(),
		keys:     keys,
	}
}

func (s *Service) Tunnel(stream pb.RelayService_TunnelServer) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("session_token")
	if len(vals) == 0 {
		return status.Error(codes.Unauthenticated, "missing session_token")
	}

	claims, err := authn.Verify(s.keys, vals[0])
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	sess, isSecond, err := s.registry.Attach(claims.SessionID, claims.Role, stream)
	if err != nil {
		return status.Errorf(codes.AlreadyExists, "%v", err)
	}
	defer s.registry.Remove(claims.SessionID)

	ctx, cancel := context.WithTimeout(stream.Context(), handshakeTimeout)
	defer cancel()

	select {
	case <-sess.paired:
	case <-ctx.Done():
		return status.Error(codes.DeadlineExceeded, "relay: timed out waiting for peer")
	}

	if isSecond {
		// Only the second caller starts the copy goroutines.
		errCh := make(chan error, 2)
		go func() {
			_, err := io.Copy(StreamWriter{Stream: sess.agent.stream}, &StreamReader{Stream: sess.client.stream})
			errCh <- err
		}()
		go func() {
			_, err := io.Copy(StreamWriter{Stream: sess.client.stream}, &StreamReader{Stream: sess.agent.stream})
			errCh <- err
		}()
		<-errCh
		cancel()
		close(sess.done)
	} else {
		// First caller waits for the tunnel to finish.
		select {
		case <-sess.done:
		case <-stream.Context().Done():
		}
	}

	return nil
}
