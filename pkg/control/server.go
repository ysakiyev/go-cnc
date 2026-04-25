package control

import (
	"net"
	"time"

	"go-cnc2/config"
	"go-cnc2/pkg/common"
	"go-cnc2/pkg/conn"
	"go-cnc2/pkg/services"
	pb "go-cnc2/proto/pb"

	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	conf          config.ControlConf
	agentService  *services.AgentService
	clientService *services.ClientService
}

func NewServer(conf config.ControlConf) *Server {
	keys := signingKeys(conf.SigningKey)
	cm := conn.NewConnManager()
	return &Server{
		conf:          conf,
		agentService:  services.NewAgentService(cm),
		clientService: services.NewClientService(cm, conf.RelayAddr, keys),
	}
}

func (s *Server) Run() error {
	listener, err := net.Listen("tcp", s.conf.Address)
	if err != nil {
		logger.Fatalf(common.StrCannotStartSrv, err)
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             s.conf.KaepMinTime * time.Second,
		PermitWithoutStream: s.conf.KaepPermitWithoutStream,
	}
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     s.conf.KaspMaxConnectionIdle * time.Minute,
		MaxConnectionAgeGrace: s.conf.KaspMaxConnectionAgeGrace * time.Minute,
		Time:                  s.conf.KaspTime * time.Hour,
		Timeout:               s.conf.KaspTimeout * time.Second,
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	pb.RegisterAgentServiceServer(grpcServer, s.agentService)
	pb.RegisterClientServiceServer(grpcServer, s.clientService)
	reflection.Register(grpcServer)

	logger.Infof(common.StrInitGrpcSrv, listener.Addr().String())
	return grpcServer.Serve(listener)
}

func signingKeys(key string) map[byte][]byte {
	return map[byte][]byte{0: []byte(key)}
}
