# go-cnc2
Command and control: server, client, agent

## Quick Start
1. Run server
2. Agents connect to server
3. User at client requests list of connected agents from server
4. User at client selects an agent from list
5. Client opens socks5 proxy
6. On user open connect to remote addr via socks5 proxy:
    client makes request ClientConnRequest with {agentId} and {remoteAddr}
    on response, client gets {connId}
7. Client makes request to create bidirectional tcp stream and
runs 2 goroutines for handling both directions

## Architecture Overview

Go-CNC2 is a sophisticated Command & Control proxy system with a three-tier architecture using multiple networking layers. The system enables remote network access through controlled agents while maintaining standard SOCKS5 compatibility for client applications.

### Core Components

#### 1. **Server** (`cmd/server/main.go`, `pkg/server/server.go`)
- Central command & control hub
- gRPC server with keepalive configuration
- Manages agent connections and client requests
- Routes traffic between clients and agents

#### 2. **Agent** (`cmd/agent/main.go`, `pkg/agent/`)
- Remote endpoint that connects to target services
- Registers with server using unique UUID
- Creates actual TCP connections to target hosts
- Bridges gRPC streams to raw TCP connections

#### 3. **Client** (`cmd/client/main.go`, `pkg/client/`)
- User interface with interactive menu
- Runs SOCKS5 proxy server on localhost:9999
- Lists available agents and manages connections

## Network Layers & Protocol Stack

### Layer 1: **TCP Foundation**
- All components use TCP for reliable transport
- Client SOCKS5 proxy: `net.Listen("tcp", ":9999")`
- Agent target connections: `net.Dial("tcp", remoteAddr)`
- Raw byte handling with 10KB buffers

### Layer 2: **gRPC Communication**
The protocol buffer definition defines two main services:
- **ClientService**: Client ↔ Server communication
  - `GetAgents()`: List connected agents
  - `CreateConn()`: Request new connection via specific agent
  - `CreateTcpStream()`: Bidirectional data streaming
- **AgentService**: Server ↔ Agent communication
  - `CreateConnStream()`: Server pushes connection requests to agents
  - `CreateTcpStream()`: Bidirectional data streaming

### Layer 3: **SOCKS5 Proxy Protocol**
Client implements full SOCKS5 specification:
- Authentication negotiation: `{0x05, 0x00}` (no auth)
- Address parsing for both IPv4 (`0x01`) and domain names (`0x03`)
- Connection establishment response
- Binary protocol handling for IP addresses and ports

## Connection Flow & Data Path

### Connection Establishment:
1. **Agent Registration**: Agent connects to server with UUID metadata
2. **Client Query**: Client requests agent list from server
3. **Agent Selection**: User selects target agent via interactive menu
4. **SOCKS5 Activation**: Client starts listening on port 9999

### Data Flow:
1. **SOCKS5 Request**: User app connects to localhost:9999
2. **Protocol Negotiation**: SOCKS5 handshake extracts target address
3. **gRPC Coordination**: Client sends `ClientConnRequest{agentId, remoteAddr}` to server
4. **Agent Notification**: Server streams `AgentConn{connId, remoteAddr}` to agent
5. **TCP Connection**: Agent dials actual target host
6. **Dual Streaming**: Bidirectional gRPC streams tunnel raw TCP data

### Data Tunneling (4 concurrent goroutines per connection):
- **Client → Agent**: `handleConnToTcpStream()` reads SOCKS5 socket, sends gRPC chunks
- **Agent → Client**: `handleTcpStreamToConn()` receives gRPC chunks, writes SOCKS5 socket
- **Agent → Target**: `handleConnToTcpStream()` reads TCP target, sends gRPC chunks
- **Target → Agent**: `handleTcpStreamToConn()` receives gRPC chunks, writes TCP target

## Key Features

- **Connection Multiplexing**: Multiple SOCKS5 connections through single agent
- **Protocol Transparency**: Raw TCP data preserved through gRPC chunking
- **Interactive Management**: Real-time agent selection and proxy control
- **Robust Error Handling**: Connection cleanup and context cancellation
- **Metadata Routing**: Connection IDs and agent IDs in gRPC metadata

## Data Flow Summary

The system creates a powerful proxy chain: `User App → SOCKS5 → gRPC → Agent → Target`, enabling remote network access through controlled agents while maintaining the simplicity of standard SOCKS5 configuration for client applications.

