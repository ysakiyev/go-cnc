package relay

import (
	pb "go-cnc2/proto/pb"
)

// chunkStream is satisfied by both RelayService_TunnelServer and RelayService_TunnelClient.
type chunkStream interface {
	Send(*pb.Chunk) error
	Recv() (*pb.Chunk, error)
}

// StreamWriter wraps a chunkStream as an io.Writer.
type StreamWriter struct {
	Stream chunkStream
}

func (w StreamWriter) Write(p []byte) (int, error) {
	if err := w.Stream.Send(&pb.Chunk{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// StreamReader wraps a chunkStream as an io.Reader, buffering leftover bytes.
type StreamReader struct {
	Stream chunkStream
	buf    []byte
}

func (r *StreamReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		chunk, err := r.Stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = chunk.Data
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
