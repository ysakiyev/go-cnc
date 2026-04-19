package services

import (
	"fmt"
	"go-cnc2/pkg/common"
	"go-cnc2/pkg/conn"
	"go-cnc2/proto/pb"

	"github.com/google/uuid"
	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AgentService struct {
	connManager *conn.ConnManager
}

func NewAgentService(connManger *conn.ConnManager) *AgentService {
	return &AgentService{connManager: connManger}
}

func (s *AgentService) CreateConnStream(_ *pb.Empty, connStream pb.AgentService_CreateConnStreamServer) error {
	md, ok := metadata.FromIncomingContext(connStream.Context())
	if !ok {
		logger.Errorf(common.LogfmtErr, common.StrSvcAgent, common.StrConnStream, common.StrErrGetMeta)
		return fmt.Errorf(common.StrErrGetMeta)
	}
	agentId, err := uuid.Parse(md.Get("agent_id")[0])
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcAgent, common.StrConnStream, common.StrErrUnableParseUuid)
		return status.Errorf(codes.InvalidArgument, common.StrErrUnableParseUuid, err)
	}

	// Register agent in connection manager
	s.connManager.RegisterAgent(agentId, connStream)
	logger.Infof(common.LogfmtInfo, common.StrSvcAgent, common.StrConnStream, fmt.Sprintf(common.StrAgentConn, agentId))

	<-connStream.Context().Done()
	// Unregister agent in connection manager
	s.connManager.UnregisterAgent(agentId)
	logger.Infof(common.LogfmtInfo, common.StrSvcAgent, common.StrConnStream, common.StrAgentDisconn)

	return connStream.Context().Err()
}

func (s *AgentService) CreateTcpStream(tcpStream pb.AgentService_CreateTcpStreamServer) error {
	md, ok := metadata.FromIncomingContext(tcpStream.Context())
	if !ok {
		logger.Errorf(common.LogfmtErr, common.StrSvcAgent, common.StrTcpStream, common.StrErrGetMeta)
		return fmt.Errorf(common.StrErrGetMeta)
	}
	connId, err := uuid.Parse(md.Get("conn_id")[0])
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcAgent, common.StrTcpStream, common.StrErrUnableParseUuid)
		return status.Errorf(codes.InvalidArgument, common.StrErrUnableParseUuid, err)
	}

	err = s.connManager.RegisterAgentTcpStream(connId, tcpStream)
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcAgent, common.StrTcpStream, fmt.Sprintf(common.StrErrRegisterAgentTcpStream, err))
		return status.Errorf(codes.InvalidArgument, common.StrErrRegisterAgentTcpStream, err)
	}

	logger.Infof(common.LogfmtInfo, common.StrSvcAgent, common.StrTcpStream, fmt.Sprintf(common.StrCreateTcpStream, connId))

	<-tcpStream.Context().Done()
	logger.Infof(common.LogfmtInfo, common.StrSvcAgent, common.StrTcpStream, common.StrCtxDone)
	return tcpStream.Context().Err()
}
