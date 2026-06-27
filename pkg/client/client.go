package client

import (
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"
	"tumbleweed/pkg/protocol"
)

// Client handles connection and messaging protocol with the Tumbleweed broker.
type Client struct {
	addr  string
	conn  net.Conn
	reqID uint32
}

// NewClient creates a new Tumbleweed client.
func NewClient(addr string) *Client {
	return &Client{
		addr: addr,
	}
}

// Connect dials the TCP connection to the broker.
func (c *Client) Connect() error {
	resolvedAddr := normalizeAddr(c.addr, ":8765")
	conn, err := net.Dial("tcp", resolvedAddr)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// normalizeAddr ensures the address has a host and a port. If port is missing, it appends the default port.
func normalizeAddr(addr string, defaultPort string) string {
	if addr == "" {
		return "localhost" + defaultPort
	}
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	
	_, _, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.Contains(addr, ":") && !strings.Contains(addr, "[") {
			return "[" + addr + "]" + defaultPort
		}
		return addr + defaultPort
	}
	return addr
}

// Close closes the connection to the broker.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) nextReqID() uint32 {
	return atomic.AddUint32(&c.reqID, 1)
}

func (c *Client) execute(reqType byte, body []byte) (*protocol.Frame, error) {
	reqID := c.nextReqID()
	reqFrame := &protocol.Frame{
		Type:   reqType,
		ReqID:  reqID,
		Length: uint32(len(body)),
		Body:   body,
	}

	if err := protocol.WriteFrame(c.conn, reqFrame); err != nil {
		return nil, fmt.Errorf("failed to write frame: %w", err)
	}

	respFrame, err := protocol.ReadFrame(c.conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read frame: %w", err)
	}

	if respFrame.Type == protocol.TypeRespError {
		errResp, err := protocol.UnmarshalErrorResponse(respFrame.Body)
		if err != nil {
			return nil, fmt.Errorf("error response unmarshal failed: %w", err)
		}
		return nil, fmt.Errorf("broker error (code %d): %s", errResp.Code, errResp.Message)
	}

	return respFrame, nil
}

// Ping sends a Ping request and returns the latency duration.
func (c *Client) Ping() (time.Duration, error) {
	start := time.Now()
	resp, err := c.execute(protocol.TypeReqPing, nil)
	if err != nil {
		return 0, err
	}
	if resp.Type != protocol.TypeRespPong {
		return 0, fmt.Errorf("unexpected response type: 0x%02x", resp.Type)
	}
	return time.Since(start), nil
}

// CreateTopic sends a CreateTopic request.
func (c *Client) CreateTopic(topic string) error {
	req := &protocol.CreateTopicRequest{Topic: topic}
	resp, err := c.execute(protocol.TypeReqCreateTopic, req.Marshal())
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeRespCreateTopic {
		return fmt.Errorf("unexpected response type: 0x%02x", resp.Type)
	}
	return nil
}

// Produce publishes a message with key and value to a topic, returning the offset.
func (c *Client) Produce(topic string, key, value []byte) (uint64, error) {
	req := &protocol.ProduceRequest{Topic: topic, Key: key, Value: value}
	resp, err := c.execute(protocol.TypeReqProduce, req.Marshal())
	if err != nil {
		return 0, err
	}
	if resp.Type != protocol.TypeRespProduce {
		return 0, fmt.Errorf("unexpected response type: 0x%02x", resp.Type)
	}
	prodResp, err := protocol.UnmarshalProduceResponse(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal produce response: %w", err)
	}
	return prodResp.Offset, nil
}

// Fetch retrieves messages matching the FetchRequest.
func (c *Client) Fetch(req *protocol.FetchRequest) ([]protocol.Message, error) {
	resp, err := c.execute(protocol.TypeReqFetch, req.Marshal())
	if err != nil {
		return nil, err
	}
	if resp.Type != protocol.TypeRespFetch {
		return nil, fmt.Errorf("unexpected response type: 0x%02x", resp.Type)
	}
	fetchResp, err := protocol.UnmarshalFetchResponse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal fetch response: %w", err)
	}
	return fetchResp.Messages, nil
}

// Ack acknowledges a message offset for a topic and consumer group.
func (c *Client) Ack(topic, group string, offset uint64) error {
	req := &protocol.AckRequest{Topic: topic, Group: group, Offset: offset}
	resp, err := c.execute(protocol.TypeReqAck, req.Marshal())
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeRespAck {
		return fmt.Errorf("unexpected response type: 0x%02x", resp.Type)
	}
	return nil
}

// Nack negative-acknowledges a message offset for a topic and consumer group.
func (c *Client) Nack(topic, group string, offset uint64) error {
	req := &protocol.NackRequest{Topic: topic, Group: group, Offset: offset}
	resp, err := c.execute(protocol.TypeReqNack, req.Marshal())
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeRespNack {
		return fmt.Errorf("unexpected response type: 0x%02x", resp.Type)
	}
	return nil
}

// ListTopics retrieves list of all topics in the broker.
func (c *Client) ListTopics() ([]string, error) {
	resp, err := c.execute(protocol.TypeReqListTopics, nil)
	if err != nil {
		return nil, err
	}
	if resp.Type != protocol.TypeRespListTopics {
		return nil, fmt.Errorf("unexpected response type: 0x%02x", resp.Type)
	}
	listResp, err := protocol.UnmarshalListTopicsResponse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal list topics response: %w", err)
	}
	return listResp.Topics, nil
}

