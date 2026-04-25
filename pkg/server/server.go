package server

import (
	"go-cnc2/config"
	"go-cnc2/pkg/common"
	"go-cnc2/pkg/conn"
	"go-cnc2/pkg/relay"
	"go-cnc2/pkg/services"
	"go-cnc2/proto/pb"
	"log"
	"net"
	"time"

	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	Address       string
	CM            *conn.ConnManager
	AgentService  *services.AgentService
	ClientService *services.ClientService
}

func NewServer(conf config.Conf) *Server {
	keys := map[byte][]byte{
		0: []byte("your-32-byte-signing-key-here!!!"),
	}
	cm := conn.NewConnManager()
	as := services.NewAgentService(cm)
	cs := services.NewClientService(cm, conf.Server.Address, keys)
	return &Server{
		Address:       conf.Server.Address,
		CM:            cm,
		AgentService:  as,
		ClientService: cs,
	}
}

func (s *Server) Run(conf config.ServerConf) error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		log.Fatal(common.StrCannotStartSrv, err)
	}

	// Keepalive enforcement policy
	var kaep = keepalive.EnforcementPolicy{
		MinTime:             conf.KaepMinTime * time.Second,
		PermitWithoutStream: true,
	}

	// Keepalive server parameters
	var kasp = keepalive.ServerParameters{
		MaxConnectionIdle:     conf.KaspMaxConnectionIdle * time.Minute,
		MaxConnectionAgeGrace: conf.KaspMaxConnectionAgeGrace * time.Minute,
		Time:                  conf.KaspTime * time.Hour,
		Timeout:               conf.KaspTimeout * time.Second,
	}

	serverOptions := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	}

	// Registering services in gRPC server
	grpcServer := grpc.NewServer(serverOptions...)
	pb.RegisterAgentServiceServer(grpcServer, s.AgentService)
	pb.RegisterClientServiceServer(grpcServer, s.ClientService)
	reflection.Register(grpcServer)

	relayKeys := map[byte][]byte{
		0: []byte("your-32-byte-signing-key-here!!!"),
	}
	relay.Register(grpcServer, relayKeys)

	logger.Info(common.StrInitGrpcSrv, listener.Addr().String())

	return grpcServer.Serve(listener)
}
