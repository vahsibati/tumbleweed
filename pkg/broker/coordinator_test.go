package broker

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"tumbleweed/pkg/config"
	"tumbleweed/pkg/protocol"
)

func TestBrokerBasicProduceFetch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-broker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	topic := "tasks"
	key := []byte("k1")
	val := []byte("v1")

	o, err := b.Produce(topic, key, val)
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}
	if o != 0 {
		t.Errorf("expected offset 0, got %d", o)
	}

	// Fetch immediately
	req := &protocol.FetchRequest{
		Topic:             topic,
		Group:             "worker-group",
		ConsumerID:        "c1",
		MaxMessages:       10,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 0,
	}

	msgs, err := b.Fetch(req)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	m := msgs[0]
	if m.Offset != 0 || !bytes.Equal(m.Key, key) || !bytes.Equal(m.Value, val) {
		t.Errorf("mismatch: %+v", m)
	}
}

func TestBrokerCompetingConsumers(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-competing-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	topic := "tasks"
	b.Produce(topic, nil, []byte("msg1"))
	b.Produce(topic, nil, []byte("msg2"))

	// Consumer 1 fetches 1 message
	req1 := &protocol.FetchRequest{
		Topic:             topic,
		Group:             "g1",
		ConsumerID:        "c1",
		MaxMessages:       1,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 0,
	}
	m1, _ := b.Fetch(req1)
	if len(m1) != 1 || string(m1[0].Value) != "msg1" {
		t.Errorf("c1 expected msg1, got %v", m1)
	}

	// Consumer 2 fetches 1 message (should get msg2 because msg1 is leased to c1)
	req2 := &protocol.FetchRequest{
		Topic:             topic,
		Group:             "g1",
		ConsumerID:        "c2",
		MaxMessages:       1,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 0,
	}
	m2, _ := b.Fetch(req2)
	if len(m2) != 1 || string(m2[0].Value) != "msg2" {
		t.Errorf("c2 expected msg2, got %v", m2)
	}
}

func TestBrokerAckSlidingWindow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-sliding-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	topic := "tasks"
	b.Produce(topic, nil, []byte("m0"))
	b.Produce(topic, nil, []byte("m1"))
	b.Produce(topic, nil, []byte("m2"))

	req := &protocol.FetchRequest{
		Topic:             topic,
		Group:             "g1",
		ConsumerID:        "c1",
		MaxMessages:       3,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 0,
	}

	msgs, _ := b.Fetch(req)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 msgs, got %d", len(msgs))
	}

	// Ack m1 (offset 1) - Committed offset should not advance
	err = b.Ack(topic, "g1", 1)
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	topicObj := b.topics[topic]
	groupObj := topicObj.groups["g1"]

	groupObj.mu.Lock()
	if groupObj.CommittedOffset != 0 {
		t.Errorf("expected committed offset to be 0, got %d", groupObj.CommittedOffset)
	}
	groupObj.mu.Unlock()

	// Ack m0 (offset 0) - Committed offset should advance past offset 1 to 2
	err = b.Ack(topic, "g1", 0)
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	groupObj.mu.Lock()
	if groupObj.CommittedOffset != 2 {
		t.Errorf("expected committed offset to advance to 2, got %d", groupObj.CommittedOffset)
	}
	groupObj.mu.Unlock()
}

func TestBrokerNackAndRedelivery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-nack-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	topic := "tasks"
	b.Produce(topic, nil, []byte("nack-msg"))

	req := &protocol.FetchRequest{
		Topic:             topic,
		Group:             "g1",
		ConsumerID:        "c1",
		MaxMessages:       1,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 0,
	}

	msgs, _ := b.Fetch(req)
	if len(msgs) != 1 {
		t.Fatalf("failed to fetch message")
	}

	// Nack it
	err = b.Nack(topic, "g1", 0)
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}

	// Fetch again, should be available immediately
	msgs2, _ := b.Fetch(req)
	if len(msgs2) != 1 || msgs2[0].Offset != 0 {
		t.Fatalf("expected to get redelivered msg at offset 0, got %v", msgs2)
	}
}

func TestBrokerLeaseExpiryAndDLQ(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-dlq-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.MaxRedeliveries = 1

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer b.Close()

	topic := "tasks"
	b.Produce(topic, nil, []byte("dead-letter"))

	req := &protocol.FetchRequest{
		Topic:             topic,
		Group:             "g1",
		ConsumerID:        "c1",
		MaxMessages:       1,
		AckTimeoutMs:      10, // ultra short lease timeout
		LongPollTimeoutMs: 0,
	}

	// Fetch 1: redelivery count = 0
	b.Fetch(req)

	// Wait for expiry
	time.Sleep(30 * time.Millisecond)

	// Fetch 2: redelivery count = 1
	b.Fetch(req)

	// Wait for expiry
	time.Sleep(30 * time.Millisecond)

	// Fetch 3: should send message to DLQ (tasks-dlq) and mark it as acknowledged
	msgs, _ := b.Fetch(req)
	if len(msgs) != 0 {
		t.Errorf("expected no messages since it should be sent to DLQ, got %v", msgs)
	}

	// Verify the DLQ topic exists and has the message
	dlqTopic := fmt.Sprintf("%s-dlq", topic)
	reqDlq := &protocol.FetchRequest{
		Topic:             dlqTopic,
		Group:             "dlq-inspector",
		ConsumerID:        "inspector",
		MaxMessages:       1,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 0,
	}

	dlqMsgs, err := b.Fetch(reqDlq)
	if err != nil {
		t.Fatalf("failed to fetch from DLQ: %v", err)
	}

	if len(dlqMsgs) != 1 || string(dlqMsgs[0].Value) != "dead-letter" {
		t.Errorf("expected DLQ to contain 'dead-letter', got %v", dlqMsgs)
	}
}
