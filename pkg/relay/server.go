package relay

import (
	pb "go-cnc2/proto/pb"

	"google.golang.org/grpc"
)

func Register(s *grpc.Server, keys map[byte][]byte) {
	pb.RegisterRelayServiceServer(s, NewService(keys))
}
