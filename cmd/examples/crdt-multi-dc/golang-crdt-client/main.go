package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ankur-anand/unisondb/examples/golang-crdt-client/client"
	"github.com/ankur-anand/unisondb/examples/golang-crdt-client/crdt"
)

// Check the relayer config.toml
const (
	relayerZMQDC1  = "tcp://localhost:5555"
	relayerZMQDC2  = "tcp://localhost:5556"
	relayerHTTPAPI = "http://localhost:8003/api/v1"
)

func main() {
	printBanner()

	stateManager := crdt.NewStateManager()

	httpClient := client.NewHTTPClient(relayerHTTPAPI, stateManager)

	namespaces := map[string]string{
		"ad-campaign-dc1": relayerZMQDC1,
		"ad-campaign-dc2": relayerZMQDC2,
	}

	zmqListener := client.NewZMQListener(namespaces, stateManager, httpClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n\nShutting down...")
		cancel()
		zmqListener.Stop()
		os.Exit(0)
	}()

	fmt.Println("Waiting for change notifications...")

	if err := zmqListener.Start(ctx); err != nil {
		fmt.Printf("Fatal error: %v\n", err)
		os.Exit(1)
	}
}

func printBanner() {
	banner := "=========================================================\n" +
		"        UnisonDB CRDT Client - Multi-DC Example\n" +
		"=========================================================\n"
	fmt.Print(banner)
}
