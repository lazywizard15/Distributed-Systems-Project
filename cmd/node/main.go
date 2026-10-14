package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"distributed-banking/internal/api"
	"distributed-banking/internal/bank"
	"distributed-banking/internal/raft"

	"github.com/gin-gonic/gin"
)

func main() {
	// ── Configuration from environment variables ──────────────────
	// Each node is started with:
	//   NODE_ID=node1
	//   PORT=8080
	//   REDIS_ADDR=redis1:6379
	//   PEERS=node2:8081,node3:8082
	nodeID := getEnv("NODE_ID", "node1")
	port := getEnv("PORT", "8080")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	peersStr := getEnv("PEERS", "")

	peers := []string{}
	if peersStr != "" {
		peers = strings.Split(peersStr, ",")
	}

	fmt.Printf("Starting node %s on port %s\n", nodeID, port)
	fmt.Printf("Peers: %v\n", peers)
	fmt.Printf("Redis: %s\n", redisAddr)

	// ── Set up Redis store ────────────────────────────────────────
	store := bank.NewStore(redisAddr)
	if err := store.Ping(); err != nil {
		log.Fatalf("Cannot connect to Redis at %s: %v", redisAddr, err)
	}
	fmt.Println("✓ Connected to Redis")

	// ── Set up Raft node ──────────────────────────────────────────
	// applyCh receives committed log entries — we apply them to Redis
	applyCh := make(chan raft.LogEntry, 100)
	node := raft.NewNode(nodeID, peers, applyCh)

	// Start a goroutine to apply committed entries to the bank store
	go func() {
		for entry := range applyCh {
			if err := bank.ApplyCommand(store, entry.Command); err != nil {
				fmt.Printf("[%s] Error applying command %q: %v\n", nodeID, entry.Command, err)
			} else {
				fmt.Printf("[%s] Applied: %s\n", nodeID, entry.Command)
			}
		}
	}()

	// Start the Raft event loop
	go node.Run()
	fmt.Println("✓ Raft node running")

	// ── Set up HTTP server ────────────────────────────────────────
	r := gin.Default()
	server := api.NewServer(node, store)
	server.SetupRoutes(r)

	fmt.Printf("✓ HTTP server listening on :%s\n", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
