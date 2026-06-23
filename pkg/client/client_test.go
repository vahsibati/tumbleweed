package client

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"tumbleweed/pkg/broker"
	"tumbleweed/pkg/config"
	"tumbleweed/pkg/protocol"
	"tumbleweed/pkg/server"
)

func TestClientSubscribeE2E(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-client-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.BindAddr = "127.0.0.1:0"

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	srv := server.NewServer(cfg, b)
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start()
	}()

	var addr string
	for i := 0; i < 20; i++ {
		time.Sleep(10 * time.Millisecond)
		if netAddr := srv.Addr(); netAddr != nil {
			addr = netAddr.String()
			break
		}
	}
	if addr == "" {
		t.Fatalf("server failed to start")
	}

	cli, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	// 1. Create topic & publish message
	topic := "tasks"
	err = cli.CreateTopic(topic)
	if err != nil {
		t.Fatalf("create topic failed: %v", err)
	}

	o1, err := cli.Publish(topic, nil, []byte("job1"))
	if err != nil {
		t.Fatalf("publish 1 failed: %v", err)
	}
	if o1 != 0 {
		t.Errorf("expected offset 0, got %d", o1)
	}

	o2, err := cli.Publish(topic, nil, []byte("job2"))
	if err != nil {
		t.Fatalf("publish 2 failed: %v", err)
	}
	if o2 != 1 {
		t.Errorf("expected offset 1, got %d", o2)
	}

	// 2. Start high-level Subscriber
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processedMu sync.Mutex
	var processed []string

	go func() {
		_ = cli.Subscribe(ctx, topic, "worker-group", "c1", func(msg protocol.Message) error {
			processedMu.Lock()
			processed = append(processed, string(msg.Value))
			processedMu.Unlock()
			if string(msg.Value) == "job2" {
				cancel() // Stop consumer once second job is processed
			}
			return nil
		})
	}()

	// Wait up to 3 seconds for processed
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatalf("test timed out waiting for consumer to process messages")
	}

	processedMu.Lock()
	defer processedMu.Unlock()

	if len(processed) != 2 {
		t.Fatalf("expected 2 processed messages, got %d: %v", len(processed), processed)
	}
	if processed[0] != "job1" || processed[1] != "job2" {
		t.Errorf("unexpected processed sequence: %v", processed)
	}
}

func TestClientNackHandlerE2E(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-client-nack-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.BindAddr = "127.0.0.1:0"

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	srv := server.NewServer(cfg, b)
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start()
	}()

	var addr string
	for i := 0; i < 20; i++ {
		time.Sleep(10 * time.Millisecond)
		if netAddr := srv.Addr(); netAddr != nil {
			addr = netAddr.String()
			break
		}
	}
	if addr == "" {
		t.Fatalf("server failed to start")
	}

	cli, err := NewClient(addr)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	topic := "tasks"
	cli.CreateTopic(topic)
	cli.Publish(topic, nil, []byte("failing-job"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts int
	var attemptsMu sync.Mutex

	go func() {
		_ = cli.Subscribe(ctx, topic, "worker-group", "c1", func(msg protocol.Message) error {
			attemptsMu.Lock()
			attempts++
			current := attempts
			attemptsMu.Unlock()

			if current == 1 {
				return errors.New("temporary failure") // Triggers NACK
			}
			cancel() // Stop subscriber on second try
			return nil
		})
	}()

	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatalf("test timed out waiting for nacked message redelivery")
	}

	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}
