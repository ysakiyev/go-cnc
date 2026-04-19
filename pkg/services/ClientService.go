package services

import (
	context "context"
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

type ClientService struct {
	connManager *conn.ConnManager
}

func NewClientService(connManger *conn.ConnManager) *ClientService {
	return &ClientService{connManager: connManger}
}

func (s *ClientService) GetAgents(ctx context.Context, _ *pb.Empty) (*pb.GetAgentsResponse, error) {
	pbResponse := pb.GetAgentsResponse{}

	for _, agentId := range s.connManager.GetAgents() {
		pbResponse.Agents = append(pbResponse.Agents, &pb.Agent{
			Id:   agentId.String(),
			Desc: "",
		})
	}

	logger.Infof(common.LogfmtInfo, common.StrSvcClient, common.StrGetAgents, common.StrSuccess)
	return &pbResponse, nil
}

func (s *ClientService) CreateConn(ctx context.Context, request *pb.ClientConnRequest) (*pb.CreateConnResponse, error) {
	pbResponse := pb.CreateConnResponse{}

	agentId, err := uuid.Parse(request.AgentId)
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcClient, common.StrCreateConn, fmt.Sprintf(common.StrErrUnableParseUuid, err))
		return &pbResponse, status.Errorf(codes.InvalidArgument, common.StrErrUnableParseUuid, err)
	}

	remoteAddr := request.RemoteAddr

	// creating conn
	connId, err := s.connManager.CreateConn()
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcClient, common.StrCreateConn, fmt.Sprintf(common.StrErrCreateConn, err))
		return &pbResponse, status.Errorf(codes.InvalidArgument, common.StrErrCreateConn, err)
	}

	// getting target agent stream to send conn
	agentConnStream := s.connManager.GetAgentStream(agentId)

	// Sending agent conn
	err = agentConnStream.Send(&pb.AgentConn{
		ConnId:     connId.String(),
		RemoteAddr: remoteAddr,
	})
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcClient, common.StrCreateConn, fmt.Sprintf(common.StrErrSendAgentConn, err))
		return &pbResponse, status.Errorf(codes.InvalidArgument, common.StrErrSendAgentConn, err)
	}

	pbResponse.ConnId = connId.String()

	logger.Infof(common.LogfmtInfo, common.StrSvcClient, common.StrCreateConn, common.StrSuccess)
	return &pbResponse, nil
}

func (s *ClientService) CreateTcpStream(tcpStream pb.ClientService_CreateTcpStreamServer) error {
	md, ok := metadata.FromIncomingContext(tcpStream.Context())
	if !ok {
		logger.Errorf(common.LogfmtErr, common.StrSvcClient, common.StrTcpStream, common.StrErrGetMeta)
		return fmt.Errorf(common.StrErrGetMeta)
	}
	connId, err := uuid.Parse(md.Get("conn_id")[0])
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcClient, common.StrTcpStream, common.StrErrUnableParseUuid)
		return status.Errorf(codes.InvalidArgument, common.StrErrUnableParseUuid, err)
	}

	err = s.connManager.RegisterClientTcpStream(connId, tcpStream)
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrSvcClient, common.StrTcpStream, fmt.Sprintf(common.StrErrRegisterClientTcpStream, err))
		return status.Errorf(codes.InvalidArgument, common.StrErrRegisterClientTcpStream, err)
	}

	logger.Infof(common.LogfmtInfo, common.StrSvcClient, common.StrTcpStream, fmt.Sprintf(common.StrCreateTcpStream, connId))

	<-tcpStream.Context().Done()
	logger.Infof(common.LogfmtInfo, common.StrSvcClient, common.StrTcpStream, common.StrCtxDone)
	return tcpStream.Context().Err()
}
