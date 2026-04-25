package conn

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	pb "go-cnc2/proto/pb"
)

type ConnManager struct {
	Agents     map[uuid.UUID]pb.AgentService_CreateConnStreamServer
	LockAgents sync.Mutex
}

func NewConnManager() *ConnManager {
	return &ConnManager{
		Agents: make(map[uuid.UUID]pb.AgentService_CreateConnStreamServer),
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

func (m *ConnManager) GetAgentStream(agentId uuid.UUID) (pb.AgentService_CreateConnStreamServer, error) {
	m.LockAgents.Lock()
	defer m.LockAgents.Unlock()
	stream, ok := m.Agents[agentId]
	if !ok {
		return nil, fmt.Errorf("agent %s not found", agentId)
	}
	return stream, nil
}

func (m *ConnManager) GetAgents() []uuid.UUID {
	m.LockAgents.Lock()
	defer m.LockAgents.Unlock()
	var uuids []uuid.UUID
	for id := range m.Agents {
		uuids = append(uuids, id)
	}
	return uuids
}
