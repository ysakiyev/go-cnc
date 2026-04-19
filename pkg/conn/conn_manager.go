package conn

import (
	"fmt"
	"go-cnc2/pkg/common"
	"go-cnc2/proto/pb"
	"log"
	"sync"

	"github.com/google/uuid"
	logger "github.com/sirupsen/logrus"
)

const ChanBuffSize = 10

type StreamChans struct {
	ClientToAgent chan pb.Chunk
	AgentToClient chan pb.Chunk
}

type ConnManager struct {
	Agents     map[uuid.UUID]pb.AgentService_CreateConnStreamServer // agentId : connStream
	LockAgents sync.Mutex

	Conns     map[uuid.UUID]StreamChans // connId : {clientTcpStream, agentTcpStream}
	LockConns sync.Mutex
}

func NewConnManager() *ConnManager {
	return &ConnManager{
		Agents: make(map[uuid.UUID]pb.AgentService_CreateConnStreamServer),
		Conns:  make(map[uuid.UUID]StreamChans),
	}
}

func (m *ConnManager) RegisterAgent(agentId uuid.UUID, stream pb.AgentService_CreateConnStreamServer) {
	m.LockAgents.Lock()
	defer m.LockAgents.Unlock()

	m.Agents[agentId] = stream
}

func (m *ConnManager) UnregisterAgent(agentId uuid.UUID) {
	m.LockAgents.Lock()
	defer m.LockAgents.Unlock()

	delete(m.Agents, agentId)
}

func (m *ConnManager) GetAgentStream(agentId uuid.UUID) pb.AgentService_CreateConnStreamServer {
	m.LockAgents.Lock()
	defer m.LockAgents.Unlock()

	return m.Agents[agentId]
}

func (m *ConnManager) GetAgents() []uuid.UUID {
	m.LockAgents.Lock()
	defer m.LockAgents.Unlock()

	var uuids []uuid.UUID
	for key, _ := range m.Agents {
		uuids = append(uuids, key)
	}
	return uuids
}

func (m *ConnManager) CreateConn() (uuid.UUID, error) {
	m.LockConns.Lock()
	defer m.LockConns.Unlock()

	connId, err := uuid.NewRandom()
	if err != nil {
		logger.Errorf(common.LogfmtErr, common.StrConnMngr, common.StrCreateConn, fmt.Sprintf(common.StrErrGenUuid, err))
		return uuid.UUID{}, fmt.Errorf(common.StrErrGenUuid, err)
	}

	m.Conns[connId] = StreamChans{
		ClientToAgent: make(chan pb.Chunk, ChanBuffSize),
		AgentToClient: make(chan pb.Chunk, ChanBuffSize),
	}

	return connId, nil
}

func (m *ConnManager) RegisterClientTcpStream(connId uuid.UUID, clientTcpStream pb.ClientService_CreateTcpStreamServer) error {
	m.LockConns.Lock()
	defer m.LockConns.Unlock()

	if _, ok := m.Conns[connId]; !ok {
		return fmt.Errorf("entry does not exist")
	}
	streamChans := m.Conns[connId]

	// read from clientTcpStream - put to clientToAgentChan
	go func() {
		for {
			pbChunk, err := clientTcpStream.Recv()
			if err != nil {
				logger.Errorf(common.LogfmtErr, common.StrConnMngr, common.StrErrReadTcpStream, err)
				break
			}
			log.Printf("Received from client tcp stream: % x ", pbChunk.Data)
			streamChans.ClientToAgent <- *pbChunk
		}
	}()
	// read from agentToClientChan - put to clientTcpStream
	go func() {
		for {
			pbChunk := <-streamChans.AgentToClient
			err := clientTcpStream.Send(&pbChunk)
			if err != nil {
				logger.Errorf(common.LogfmtErr, common.StrConnMngr, common.StrErrWriteTcpStream, err)
				break
			}
		}
	}()

	return nil
}

func (m *ConnManager) RegisterAgentTcpStream(connId uuid.UUID, agentTcpStream pb.AgentService_CreateTcpStreamServer) error {
	m.LockConns.Lock()
	defer m.LockConns.Unlock()

	if _, ok := m.Conns[connId]; !ok {
		return fmt.Errorf("entry does not exist")
	}
	streamChans := m.Conns[connId]

	// read from agentTcpStream - put to agentToClientChan
	go func() {
		for {
			pbChunk, err := agentTcpStream.Recv()
			if err != nil {
				logger.Errorf(common.LogfmtErr, common.StrConnMngr, common.StrErrReadTcpStream, err)
				break
			}
			log.Printf("Received from client tcp stream: % x ", pbChunk.Data)
			streamChans.AgentToClient <- *pbChunk
		}
	}()
	// read from clientToAgentChan - put to agentTcpStream
	go func() {
		for {
			pbChunk := <-streamChans.ClientToAgent
			err := agentTcpStream.Send(&pbChunk)
			if err != nil {
				logger.Errorf(common.LogfmtErr, common.StrConnMngr, common.StrErrWriteTcpStream, err)
				break
			}
		}
	}()
	return nil
}
