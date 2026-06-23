package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"tumbleweed/pkg/broker"
	"tumbleweed/pkg/config"
	"tumbleweed/pkg/protocol"
)

// Server represents the TCP server wrapping the Tumbleweed broker engine.
type Server struct {
	cfg      *config.Config
	broker   *broker.Broker
	listener net.Listener
	wg       sync.WaitGroup
	closed   bool
	mu       sync.Mutex
}

// NewServer initializes a new Server.
func NewServer(cfg *config.Config, b *broker.Broker) *Server {
	return &Server{
		cfg:    cfg,
		broker: b,
	}
}

// Start starts the TCP server listening for client connections.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.cfg.BindAddr)
	if err != nil {
		return fmt.Errorf("server: failed to listen on %s: %w", s.cfg.BindAddr, err)
	}

	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

	log.Printf("Tumbleweed broker listening on %s...", s.cfg.BindAddr)

	for {
		conn, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed {
				return nil
			}
			log.Printf("server: error accepting connection: %v", err)
			continue
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConnection(c)
		}(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return // Client disconnected normally
			}
			log.Printf("server: error reading frame: %v", err)
			return
		}

		respFrame := s.processFrame(frame)
		if err := protocol.WriteFrame(conn, respFrame); err != nil {
			log.Printf("server: error writing response frame: %v", err)
			return
		}
	}
}

func (s *Server) processFrame(f *protocol.Frame) *protocol.Frame {
	switch f.Type {
	case protocol.TypeReqPing:
		return &protocol.Frame{
			Type:  protocol.TypeRespPong,
			ReqID: f.ReqID,
		}

	case protocol.TypeReqProduce:
		req, err := protocol.UnmarshalProduceRequest(f.Body)
		if err != nil {
			return makeErrorFrame(f.ReqID, protocol.ErrCodeInvalidPayload, err.Error())
		}
		offset, err := s.broker.Produce(req.Topic, req.Key, req.Value)
		if err != nil {
			return makeErrorFrame(f.ReqID, protocol.ErrCodeInternalError, err.Error())
		}
		resp := &protocol.ProduceResponse{Offset: offset}
		return &protocol.Frame{
			Type:  protocol.TypeRespProduce,
			ReqID: f.ReqID,
			Body:  resp.Marshal(),
		}

	case protocol.TypeReqFetch:
		req, err := protocol.UnmarshalFetchRequest(f.Body)
		if err != nil {
			return makeErrorFrame(f.ReqID, protocol.ErrCodeInvalidPayload, err.Error())
		}
		msgs, err := s.broker.Fetch(req)
		if err != nil {
			code := protocol.ErrCodeInternalError
			if errors.Is(err, broker.ErrTopicNotFound) {
				code = protocol.ErrCodeTopicNotFound
			}
			return makeErrorFrame(f.ReqID, code, err.Error())
		}
		resp := &protocol.FetchResponse{Messages: msgs}
		return &protocol.Frame{
			Type:  protocol.TypeRespFetch,
			ReqID: f.ReqID,
			Body:  resp.Marshal(),
		}

	case protocol.TypeReqAck:
		req, err := protocol.UnmarshalAckRequest(f.Body)
		if err != nil {
			return makeErrorFrame(f.ReqID, protocol.ErrCodeInvalidPayload, err.Error())
		}
		err = s.broker.Ack(req.Topic, req.Group, req.Offset)
		if err != nil {
			code := protocol.ErrCodeInternalError
			if errors.Is(err, broker.ErrLeaseNotFound) {
				code = protocol.ErrCodeLeaseExpired
			} else if errors.Is(err, broker.ErrTopicNotFound) {
				code = protocol.ErrCodeTopicNotFound
			}
			return makeErrorFrame(f.ReqID, code, err.Error())
		}
		return &protocol.Frame{
			Type:  protocol.TypeRespAck,
			ReqID: f.ReqID,
		}

	case protocol.TypeReqNack:
		req, err := protocol.UnmarshalNackRequest(f.Body)
		if err != nil {
			return makeErrorFrame(f.ReqID, protocol.ErrCodeInvalidPayload, err.Error())
		}
		err = s.broker.Nack(req.Topic, req.Group, req.Offset)
		if err != nil {
			code := protocol.ErrCodeInternalError
			if errors.Is(err, broker.ErrLeaseNotFound) {
				code = protocol.ErrCodeLeaseExpired
			} else if errors.Is(err, broker.ErrTopicNotFound) {
				code = protocol.ErrCodeTopicNotFound
			}
			return makeErrorFrame(f.ReqID, code, err.Error())
		}
		return &protocol.Frame{
			Type:  protocol.TypeRespNack,
			ReqID: f.ReqID,
		}

	case protocol.TypeReqCreateTopic:
		req, err := protocol.UnmarshalCreateTopicRequest(f.Body)
		if err != nil {
			return makeErrorFrame(f.ReqID, protocol.ErrCodeInvalidPayload, err.Error())
		}
		_, err = s.broker.CreateTopic(req.Topic)
		if err != nil {
			return makeErrorFrame(f.ReqID, protocol.ErrCodeInternalError, err.Error())
		}
		return &protocol.Frame{
			Type:  protocol.TypeRespCreateTopic,
			ReqID: f.ReqID,
		}

	default:
		return makeErrorFrame(f.ReqID, protocol.ErrCodeUnknown, "unknown request type")
	}
}

func makeErrorFrame(reqID uint32, code uint16, message string) *protocol.Frame {
	resp := &protocol.ErrorResponse{
		Code:    code,
		Message: message,
	}
	return &protocol.Frame{
		Type:  protocol.TypeRespError,
		ReqID: reqID,
		Body:  resp.Marshal(),
	}
}

// Addr returns the network address the server is listening on, or nil if not listening.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Stop stops the server, closing the listener and active client connections.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}

	s.wg.Wait()
	return err
}
