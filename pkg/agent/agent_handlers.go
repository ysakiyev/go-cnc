package agent

import (
	"context"
	"fmt"
	"go-cnc2/pkg/common"
	"go-cnc2/proto/pb"
	"log"
	"net"

	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

func (a *Agent) HandleConnStream(stream pb.AgentService_CreateConnStreamClient) {
	for {
		pbConn, err := stream.Recv()
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliAgent, common.StrHandleConnStream, fmt.Sprintf(common.StrErrReadConnStream, err))
			break
		}
		logger.Infof("Received conn to remote addr: %s", pbConn.RemoteAddr)

		if pbConn.RemoteAddr == "" {
			logger.Errorf("Empty remote address for connId: %s", pbConn.ConnId)
			continue
		}

		conn, err := net.Dial("tcp", pbConn.RemoteAddr)
		if err != nil {
			logger.Errorf("Failed to dial %s: %v", pbConn.RemoteAddr, err)
			continue
		}
		logger.Infof("Successfully connected to %s", pbConn.RemoteAddr)

		ctx := context.Background()
		ctx = metadata.AppendToOutgoingContext(ctx, "conn_id", pbConn.ConnId)
		tcpStream, err := a.CreateTcpStream(ctx)
		if err != nil {
			logger.Errorf("unable to create tcp stream: %v", err)
			conn.Close()
			continue
		}
		logger.Infof("Agent created tcp stream to connId: %s", pbConn.ConnId)

		go func(c net.Conn, s pb.AgentService_CreateTcpStreamClient) {
			defer c.Close()
			a.handleTcpStreamToConn(c, s)
		}(conn, tcpStream)
		go a.handleConnToTcpStream(conn, tcpStream)
	}
}

func (a *Agent) handleTcpStreamToConn(conn net.Conn, stream pb.AgentService_CreateTcpStreamClient) {
	for {
		pbChunk, err := stream.Recv()
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliAgent, common.StrHandleTcpStream, fmt.Sprintf(common.StrErrReadTcpStream, err))
			break
		}
		logger.Infof("Received from tcp stream: % x ", pbChunk.Data)

		log.Printf("Agent writing %d bytes to target connection: % x", len(pbChunk.Data), pbChunk.Data)
		_, err = conn.Write(pbChunk.Data)
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliAgent, common.StrHandleTcpStream, fmt.Sprintf(common.StrErrWriteTcpConn, err))
			break
		}
	}
}

func (a *Agent) handleConnToTcpStream(conn net.Conn, stream pb.AgentService_CreateTcpStreamClient) {
	buff := make([]byte, 10000)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliAgent, common.StrHandleTcpStream, fmt.Sprintf(common.StrErrReadTcpConn, err))
			break
		}
		log.Printf("Agent read %d bytes from target, sending to stream: % x", n, buff[:n])
		pbChunk := pb.Chunk{Data: buff[:n]}
		err = stream.Send(&pbChunk)
		if err != nil {
			logger.Errorf(common.LogfmtErr, common.StrCliAgent, common.StrHandleTcpStream, fmt.Sprintf(common.StrErrWriteTcpStream, err))
			break
		}
	}
}
