package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"tumbleweed/pkg/protocol"
)

const (
	defaultDialTimeout       = 5 * time.Second
	defaultMaxMessages       = 5
	defaultAckTimeout        = 30 * time.Second
	defaultLongPollTimeout   = 2 * time.Second
	defaultRetryBackoffDelay = 1 * time.Second
)

// Client handles communications with the Tumbleweed message broker.
type Client struct {
	addr     string
	conn     net.Conn
	mu       sync.Mutex
	reqIDSeq uint32
	closed   bool
}

// NewClient creates a new Tumbleweed client.
func NewClient(addr string) (*Client, error) {
	c := &Client{addr: addr}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	conn, err := net.DialTimeout("tcp", c.addr, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("client: failed to connect to %s: %w", c.addr, err)
	}
	c.conn = conn
	return nil
}

func (c *Client) execute(reqType byte, payload []byte) (*protocol.Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("client: closed")
	}

	reqID := atomic.AddUint32(&c.reqIDSeq, 1)
	frame := &protocol.Frame{
		Type:  reqType,
		ReqID: reqID,
		Body:  payload,
	}

	// Helper to send frame, reconnecting once if disconnected
	sendAndRead := func() (*protocol.Frame, error) {
		if c.conn == nil {
			if err := c.connect(); err != nil {
				return nil, err
			}
		}

		err := protocol.WriteFrame(c.conn, frame)
		if err != nil {
			c.conn.Close()
			c.conn = nil
			// Try once to reconnect
			if err := c.connect(); err != nil {
				return nil, err
			}
			if err := protocol.WriteFrame(c.conn, frame); err != nil {
				return nil, err
			}
		}

		resp, err := protocol.ReadFrame(c.conn)
		if err != nil {
			c.conn.Close()
			c.conn = nil
			return nil, err
		}

		return resp, nil
	}

	resp, err := sendAndRead()
	if err != nil {
		return nil, err
	}

	if resp.Type == protocol.TypeRespError {
		errResp, err := protocol.UnmarshalErrorResponse(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("client: failed to unmarshal error response: %w", err)
		}
		return nil, fmt.Errorf("broker error (code %d): %s", errResp.Code, errResp.Message)
	}

	return resp, nil
}

// CreateTopic explicitly creates a topic on the broker.
func (c *Client) CreateTopic(topic string) error {
	req := &protocol.CreateTopicRequest{Topic: topic}
	resp, err := c.execute(protocol.TypeReqCreateTopic, req.Marshal())
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeRespCreateTopic {
		return fmt.Errorf("client: unexpected response type: %x", resp.Type)
	}
	return nil
}

// Publish publishes a message key and value to a topic, returning the assigned offset.
func (c *Client) Publish(topic string, key, value []byte) (uint64, error) {
	req := &protocol.ProduceRequest{
		Topic: topic,
		Key:   key,
		Value: value,
	}
	resp, err := c.execute(protocol.TypeReqProduce, req.Marshal())
	if err != nil {
		return 0, err
	}
	if resp.Type != protocol.TypeRespProduce {
		return 0, fmt.Errorf("client: unexpected response type: %x", resp.Type)
	}

	prodResp, err := protocol.UnmarshalProduceResponse(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("client: failed to unmarshal produce response: %w", err)
	}

	return prodResp.Offset, nil
}

// Fetch requests a batch of messages from the broker.
func (c *Client) Fetch(topic, group, consumerID string, maxMessages uint32, ackTimeout, longPollTimeout time.Duration) ([]protocol.Message, error) {
	req := &protocol.FetchRequest{
		Topic:             topic,
		Group:             group,
		ConsumerID:        consumerID,
		MaxMessages:       maxMessages,
		AckTimeoutMs:      uint32(ackTimeout.Milliseconds()),
		LongPollTimeoutMs: uint32(longPollTimeout.Milliseconds()),
	}

	resp, err := c.execute(protocol.TypeReqFetch, req.Marshal())
	if err != nil {
		return nil, err
	}
	if resp.Type != protocol.TypeRespFetch {
		return nil, fmt.Errorf("client: unexpected response type: %x", resp.Type)
	}

	fetchResp, err := protocol.UnmarshalFetchResponse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client: failed to unmarshal fetch response: %w", err)
	}

	return fetchResp.Messages, nil
}

// Ack acknowledges a message offset.
func (c *Client) Ack(topic, group string, offset uint64) error {
	req := &protocol.AckRequest{
		Topic:  topic,
		Group:  group,
		Offset: offset,
	}
	resp, err := c.execute(protocol.TypeReqAck, req.Marshal())
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeRespAck {
		return fmt.Errorf("client: unexpected response type: %x", resp.Type)
	}
	return nil
}

// Nack rejects a message, marking it for immediate redelivery.
func (c *Client) Nack(topic, group string, offset uint64) error {
	req := &protocol.NackRequest{
		Topic:  topic,
		Group:  group,
		Offset: offset,
	}
	resp, err := c.execute(protocol.TypeReqNack, req.Marshal())
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeRespNack {
		return fmt.Errorf("client: unexpected response type: %x", resp.Type)
	}
	return nil
}

// Subscribe runs a continuous loop fetching and processing messages.
// It auto-acks on success and auto-nacks on error.
func (c *Client) Subscribe(ctx context.Context, topic, group, consumerID string, handler func(msg protocol.Message) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgs, err := c.Fetch(topic, group, consumerID, defaultMaxMessages, defaultAckTimeout, defaultLongPollTimeout)
		if err != nil {
			// Backoff on connection error before retrying
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(defaultRetryBackoffDelay):
				continue
			}
		}

		for _, m := range msgs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			err := handler(m)
			if err == nil {
				_ = c.Ack(topic, group, m.Offset)
			} else {
				_ = c.Nack(topic, group, m.Offset)
			}
		}
	}
}

// Close closes the client's socket connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
