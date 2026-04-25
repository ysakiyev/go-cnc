package relay

import (
	"net"
	"time"

	"go-cnc2/config"
	"go-cnc2/pkg/common"
	pb "go-cnc2/proto/pb"

	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

func Register(s *grpc.Server, keys map[byte][]byte) {
	pb.RegisterRelayServiceServer(s, NewService(keys))
}

func Run(conf config.RelayConf) error {
	listener, err := net.Listen("tcp", conf.Address)
	if err != nil {
		logger.Fatalf(common.StrCannotStartSrv, err)
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             conf.KaepMinTime * time.Second,
		PermitWithoutStream: conf.KaepPermitWithoutStream,
	}
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     conf.KaspMaxConnectionIdle * time.Minute,
		MaxConnectionAgeGrace: conf.KaspMaxConnectionAgeGrace * time.Minute,
		Time:                  conf.KaspTime * time.Hour,
		Timeout:               conf.KaspTimeout * time.Second,
	}

	keys := map[byte][]byte{0: []byte(conf.SigningKey)}
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	Register(grpcServer, keys)
	reflection.Register(grpcServer)

	logger.Infof(common.StrInitGrpcSrv, listener.Addr().String())
	return grpcServer.Serve(listener)
}
