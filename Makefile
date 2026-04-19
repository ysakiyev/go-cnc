gen:
	protoc --proto_path=proto proto/*.proto --go_out=plugins=grpc:proto/pb

server:
	go run cmd/server/main.go -cp=/home/ysakiyev/dev/go/src/go-cnc -cn=config

client:
	go run cmd/client/main.go -server localhost:54321

agent1:
	go run cmd/agent/main.go -server localhost:54321

agent2:
	go run cmd/agent/main.go -server localhost:54321

agent3:
	go run cmd/agent/main.go -server localhost:54321

.PHONY: gen