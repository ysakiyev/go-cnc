gen:
	protoc --proto_path=proto proto/*.proto --go_out=plugins=grpc:proto/pb

build:
	go build -o bin/server  ./cmd/server
	go build -o bin/control ./cmd/control
	go build -o bin/relay   ./cmd/relay
	go build -o bin/agent   ./cmd/agent
	go build -o bin/client  ./cmd/client

server:
	go run cmd/server/main.go -cp=. -cn=config

control:
	go run cmd/control/main.go -cp=. -cn=config

relay:
	go run cmd/relay/main.go -cp=. -cn=config

client:
	go run cmd/client/main.go -server localhost:54321

agent1:
	go run cmd/agent/main.go -server localhost:54321

agent2:
	go run cmd/agent/main.go -server localhost:54321

agent3:
	go run cmd/agent/main.go -server localhost:54321

.PHONY: gen build server control relay client agent1 agent2 agent3
