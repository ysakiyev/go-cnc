package agent

import (
	"context"
	"go-cnc2/proto/pb"

	"google.golang.org/grpc"
)

type Agent struct {
	agentService pb.AgentServiceClient
}

func NewAgent(cc *grpc.ClientConn) *Agent {
	return &Agent{
		agentService: pb.NewAgentServiceClient(cc),
	}
}

func (a *Agent) CreateConnStream(ctx context.Context, empty *pb.Empty, opts ...grpc.CallOption) (pb.AgentService_CreateConnStreamClient, error) {
	stream, err := a.agentService.CreateConnStream(ctx, empty, opts...)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

