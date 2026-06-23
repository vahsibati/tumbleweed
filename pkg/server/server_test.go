package server

import (
	"net"
	"os"
	"testing"
	"time"

	"tumbleweed/pkg/broker"
	"tumbleweed/pkg/config"
	"tumbleweed/pkg/protocol"
)

func TestServerE2E(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-server-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.BindAddr = "127.0.0.1:0" // Bind to dynamic local port

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	srv := NewServer(cfg, b)

	// Start server in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start()
	}()

	// Wait for server to start listening
	var addr string
	for i := 0; i < 20; i++ {
		time.Sleep(10 * time.Millisecond)
		if netAddr := srv.Addr(); netAddr != nil {
			addr = netAddr.String()
			break
		}
	}
	if addr == "" {
		t.Fatalf("server failed to start in time")
	}

	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// 1. Create Topic
	createReq := &protocol.CreateTopicRequest{Topic: "tasks"}
	fCreate := &protocol.Frame{
		Type:  protocol.TypeReqCreateTopic,
		ReqID: 1,
		Body:  createReq.Marshal(),
	}
	if err := protocol.WriteFrame(conn, fCreate); err != nil {
		t.Fatalf("failed to write create topic: %v", err)
	}
	respFrame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read create response: %v", err)
	}
	if respFrame.Type != protocol.TypeRespCreateTopic {
		t.Fatalf("expected create topic response, got type: %x", respFrame.Type)
	}

	// 2. Produce Message
	prodReq := &protocol.ProduceRequest{
		Topic: "tasks",
		Key:   []byte("k1"),
		Value: []byte("hello-tcp"),
	}
	fProd := &protocol.Frame{
		Type:  protocol.TypeReqProduce,
		ReqID: 2,
		Body:  prodReq.Marshal(),
	}
	if err := protocol.WriteFrame(conn, fProd); err != nil {
		t.Fatalf("failed to write produce: %v", err)
	}
	respFrame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read produce response: %v", err)
	}
	if respFrame.Type != protocol.TypeRespProduce {
		t.Fatalf("expected produce response, got type: %x", respFrame.Type)
	}
	prodResp, err := protocol.UnmarshalProduceResponse(respFrame.Body)
	if err != nil {
		t.Fatalf("failed to unmarshal produce response: %v", err)
	}
	if prodResp.Offset != 0 {
		t.Errorf("expected offset 0, got %d", prodResp.Offset)
	}

	// 3. Fetch Message
	fetchReq := &protocol.FetchRequest{
		Topic:             "tasks",
		Group:             "workers",
		ConsumerID:        "c1",
		MaxMessages:       10,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 0,
	}
	fFetch := &protocol.Frame{
		Type:  protocol.TypeReqFetch,
		ReqID: 3,
		Body:  fetchReq.Marshal(),
	}
	if err := protocol.WriteFrame(conn, fFetch); err != nil {
		t.Fatalf("failed to write fetch: %v", err)
	}
	respFrame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read fetch response: %v", err)
	}
	if respFrame.Type != protocol.TypeRespFetch {
		t.Fatalf("expected fetch response, got type: %x", respFrame.Type)
	}
	fetchResp, err := protocol.UnmarshalFetchResponse(respFrame.Body)
	if err != nil {
		t.Fatalf("failed to unmarshal fetch response: %v", err)
	}
	if len(fetchResp.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(fetchResp.Messages))
	}
	m := fetchResp.Messages[0]
	if m.Offset != 0 || string(m.Value) != "hello-tcp" {
		t.Errorf("mismatch: %+v", m)
	}

	// 4. Ack Message
	ackReq := &protocol.AckRequest{
		Topic:  "tasks",
		Group:  "workers",
		Offset: 0,
	}
	fAck := &protocol.Frame{
		Type:  protocol.TypeReqAck,
		ReqID: 4,
		Body:  ackReq.Marshal(),
	}
	if err := protocol.WriteFrame(conn, fAck); err != nil {
		t.Fatalf("failed to write ack: %v", err)
	}
	respFrame, err = protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read ack response: %v", err)
	}
	if respFrame.Type != protocol.TypeRespAck {
		t.Fatalf("expected ack response, got type: %x", respFrame.Type)
	}

	// Close connection explicitly to let the server connection handler exit
	conn.Close()

	// Stop server
	srv.Stop()
	<-errChan
}
