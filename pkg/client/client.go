package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"go-cnc2/pkg/relay"
	pb "go-cnc2/proto/pb"

	"github.com/google/uuid"
	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	clientService pb.ClientServiceClient
}

func NewClient(cc *grpc.ClientConn) *Client {
	return &Client{
		clientService: pb.NewClientServiceClient(cc),
	}
}

func (c *Client) CreateConn(ctx context.Context, in *pb.ClientConnRequest, opts ...grpc.CallOption) (*pb.CreateConnResponse, error) {
	response, err := c.clientService.CreateConn(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return response, nil
}


func (c *Client) GetAgents(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (*pb.GetAgentsResponse, error) {
	response, err := c.clientService.GetAgents(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) Socks5ProxyStart(agentId uuid.UUID) {
	c.Socks5ProxyStartWithContext(context.Background(), agentId)
}

func (c *Client) Socks5ProxyStartWithContext(ctx context.Context, agentId uuid.UUID) {
	log.SetFlags(log.Ltime | log.Lshortfile)

	server, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Printf("Failed to start SOCKS5 server: %v", err)
		return
	}
	defer server.Close()
	log.Println("start accepting connections")

	// Channel to signal when to stop accepting connections
	done := make(chan bool)

	// Goroutine to handle context cancellation
	go func() {
		<-ctx.Done()
		log.Println("Context cancelled, stopping SOCKS5 server")
		server.Close()
		done <- true
	}()

	for {
		select {
		case <-done:
			return
		default:
			conn, err := server.Accept()
			if err != nil {
				// Check if error is due to server being closed
				select {
				case <-ctx.Done():
					return
				default:
					log.Println("Accept error:", err)
					return
				}
			}
			log.Printf("a new connection from:%s\n", conn.RemoteAddr().String())
			go c.socks5Proxy(conn, agentId)
		}
	}
}

func (c *Client) socks5Proxy(conn net.Conn, agentId uuid.UUID) {
	defer conn.Close()

	var b [1024]byte

	n, err := conn.Read(b[:])
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("Received: % x from %s", b[:n], conn.RemoteAddr().String())

	// 0x05 - socks5, 0x00 - no auth
	conn.Write([]byte{0x05, 0x00})

	n, err = conn.Read(b[:])
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("Received: % x from %s", b[:n], conn.RemoteAddr().String())

	// Getting dst ip address from bytes read from conn
	var addr string
	switch b[3] {
	case 0x01:
		sip := sockIP{}
		if err := binary.Read(bytes.NewReader(b[4:n]), binary.BigEndian, &sip); err != nil {
			log.Println("Request parse error")
			return
		}
		addr = sip.toAddr()
	case 0x03:
		hostLen := int(b[4])
		if n < 5+hostLen+2 {
			log.Println("Invalid SOCKS5 request: insufficient data for domain name")
			return
		}
		host := string(b[5 : 5+hostLen])
		var port uint16
		err = binary.Read(bytes.NewReader(b[5+hostLen:5+hostLen+2]), binary.BigEndian, &port)
		if err != nil {
			log.Println(err)
			return
		}
		addr = fmt.Sprintf("%s:%d", host, port)
	default:
		log.Printf("Unsupported SOCKS5 address type: 0x%02x", b[3])
		return
	}

	if addr == "" {
		log.Println("Failed to parse destination address")
		return
	}

	// making conn request to server
	log.Printf("Creating connection to: %s via agent: %s", addr, agentId.String())
	connRequest := pb.ClientConnRequest{
		AgentId:    agentId.String(),
		RemoteAddr: addr,
	}
	resp, err := c.CreateConn(context.Background(), &connRequest)
	if err != nil {
		logger.Errorf("error creating connection: %v", err)
		return
	}
	log.Printf("Connection created with connId: %s", resp.ConnId)

	conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	// dial the relay
	relayConn, err := grpc.Dial(resp.RelayAddr, grpc.WithInsecure()) //nolint:staticcheck
	if err != nil {
		logger.Errorf("failed to dial relay %s: %v", resp.RelayAddr, err)
		return
	}
	defer relayConn.Close()

	tunnelCtx := metadata.AppendToOutgoingContext(context.Background(), "session_token", resp.SessionToken)
	tunnel, err := pb.NewRelayServiceClient(relayConn).Tunnel(tunnelCtx)
	if err != nil {
		logger.Errorf("failed to open tunnel stream: %v", err)
		return
	}
	logger.Infof("Client tunnel open for connId: %s", resp.ConnId)

	errCh := make(chan error, 2)
	go func() { _, err := io.Copy(conn, &relay.StreamReader{Stream: tunnel}); errCh <- err }()
	go func() { _, err := io.Copy(relay.StreamWriter{Stream: tunnel}, conn); errCh <- err }()

	<-errCh
	log.Printf("Connection closed for connId: %s", resp.ConnId)

}

type sockIP struct {
	A, B, C, D byte
	PORT       uint16
}

func (ip sockIP) toAddr() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d", ip.A, ip.B, ip.C, ip.D, ip.PORT)
}
