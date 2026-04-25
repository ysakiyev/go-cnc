package services

import (
	context "context"
	"fmt"
	"go-cnc2/pkg/authn"
	"go-cnc2/pkg/common"
	"go-cnc2/pkg/conn"
	"go-cnc2/proto/pb"
	"time"

	"github.com/google/uuid"
	logger "github.com/sirupsen/logrus"
)

type ClientService struct {
	connManager *conn.ConnManager
	relayAddr   string
	keys        map[byte][]byte
}

func NewClientService(connManager *conn.ConnManager, relayAddr string, keys map[byte][]byte) *ClientService {
	return &ClientService{connManager: connManager, relayAddr: relayAddr, keys: keys}
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

func (s *ClientService) CreateConn(ctx context.Context, req *pb.ClientConnRequest) (*pb.CreateConnResponse, error) {
	agentId, err := uuid.Parse(req.AgentId)
	if err != nil {
		return nil, err
	}

	connId := uuid.New()
	sessionId := uuid.New()

	agentToken, err := authn.Mint(s.keys, authn.Claims{
		SessionID: sessionId,
		Role:      authn.RoleAgent,
		RelayID:   uuid.Nil,
		Exp:       time.Now().Add(45 * time.Second),
		KeyID:     0,
	})
	if err != nil {
		return nil, err
	}

	clientToken, err := authn.Mint(s.keys, authn.Claims{
		SessionID: sessionId,
		Role:      authn.RoleClient,
		RelayID:   uuid.Nil,
		Exp:       time.Now().Add(45 * time.Second),
		KeyID:     0,
	})
	if err != nil {
		return nil, err
	}

	agentStream, err := s.connManager.GetAgentStream(agentId)
	if err != nil {
		return nil, fmt.Errorf("agent not available: %w", err)
	}

	err = agentStream.Send(&pb.AgentConn{
		ConnId:       connId.String(),
		RemoteAddr:   req.RemoteAddr,
		RelayAddr:    s.relayAddr,
		SessionToken: agentToken,
	})
	if err != nil {
		return nil, err
	}

	return &pb.CreateConnResponse{
		ConnId:       connId.String(),
		RelayAddr:    s.relayAddr,
		SessionToken: clientToken,
	}, nil
}
