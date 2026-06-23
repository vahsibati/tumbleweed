package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tumbleweed/pkg/broker"
	"tumbleweed/pkg/config"
	"tumbleweed/pkg/server"
)

const banner = `
  _____                _     _                            _ 
 |_   _|   _ _ __ ___ | |__ | | _____      _____  ___  __| |
   | || | | | '_ ' _ \| '_ \| |/ _ \ \ /\ / / _ \/ _ \/ _' |
   | || |_| | | | | | | |_) | |  __/\ V  V /  __/  __/ (_| |
   |_| \__,_|_| |_| |_|_.__/|_|\___| \_/\_/ \___|\___|\__,_|
                                                            
`

func main() {
	fmt.Print(banner)
	log.Println("Initializing Tumbleweed Message Broker...")

	dataDir := flag.String("data-dir", "data", "directory where topic data is stored")
	bindAddr := flag.String("bind", ":8765", "host:port to bind the TCP server to")
	syncEvery := flag.Bool("sync-every-write", true, "call fsync on every single message publish")
	syncInt := flag.Duration("sync-interval", 200*time.Millisecond, "duration between disk syncs if sync-every-write is false")
	maxSegMB := flag.Int64("max-segment-mb", 64, "maximum size of a segment file in MB")
	leaseSec := flag.Int("lease-sec", 30, "default lease visibility timeout in seconds")
	maxRedeliv := flag.Int("max-redeliveries", 5, "maximum message redeliveries before diverting to DLQ")

	flag.Parse()

	// Load configuration
	cfg := config.DefaultConfig()
	cfg.DataDir = *dataDir
	cfg.BindAddr = *bindAddr
	cfg.SyncEveryWrite = *syncEvery
	cfg.SyncInterval = *syncInt
	cfg.MaxSegmentBytes = *maxSegMB * 1024 * 1024
	cfg.DefaultLeaseTimeout = time.Duration(*leaseSec) * time.Second
	cfg.MaxRedeliveries = *maxRedeliv

	log.Printf("Configuration loaded:")
	log.Printf("  - Data Directory:      %s", cfg.DataDir)
	log.Printf("  - Bind Address:        %s", cfg.BindAddr)
	log.Printf("  - Sync Every Write:    %t", cfg.SyncEveryWrite)
	log.Printf("  - Async Sync Interval: %s", cfg.SyncInterval)
	log.Printf("  - Max Segment Size:    %d MB", *maxSegMB)
	log.Printf("  - Default Lease:       %s", cfg.DefaultLeaseTimeout)
	log.Printf("  - Max Redeliveries:    %d", cfg.MaxRedeliveries)

	// Create broker engine
	b, err := broker.NewBroker(cfg)
	if err != nil {
		log.Fatalf("Fatal error starting broker: %v", err)
	}

	// Create TCP server wrapper
	srv := server.NewServer(cfg, b)

	// Signal channel for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v. Initiating graceful shutdown...", sig)

		// 1. Stop accepting connections
		if err := srv.Stop(); err != nil {
			log.Printf("Error stopping TCP server: %v", err)
		}

		// 2. Close broker (flushes WAL segments and commits offset states)
		if err := b.Close(); err != nil {
			log.Printf("Error closing broker engine: %v", err)
		}

		log.Println("Graceful shutdown complete. Goodbye!")
		os.Exit(0)
	}()

	// Start TCP server
	if err := srv.Start(); err != nil {
		log.Fatalf("Fatal server error: %v", err)
	}
}
