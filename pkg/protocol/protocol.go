package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// Frame types
const (
	TypeReqPing        byte = 0x01
	TypeReqProduce     byte = 0x02
	TypeReqFetch       byte = 0x03
	TypeReqAck         byte = 0x04
	TypeReqNack        byte = 0x05
	TypeReqCreateTopic byte = 0x06

	TypeRespPong        byte = 0x81
	TypeRespProduce     byte = 0x82
	TypeRespFetch       byte = 0x83
	TypeRespAck         byte = 0x84
	TypeRespNack        byte = 0x85
	TypeRespCreateTopic byte = 0x86
	TypeRespError       byte = 0xFF
)

// Error codes
const (
	ErrCodeUnknown        uint16 = 1000
	ErrCodeTopicNotFound  uint16 = 1001
	ErrCodeTopicExists    uint16 = 1002
	ErrCodeInvalidPayload uint16 = 1003
	ErrCodeInternalError  uint16 = 1004
	ErrCodeLeaseExpired   uint16 = 1005
)

// Frame represents a raw protocol packet on the wire.
type Frame struct {
	Type   byte
	ReqID  uint32
	Length uint32
	Body   []byte
}

// ReadFrame reads a Frame from the reader.
func ReadFrame(r io.Reader) (*Frame, error) {
	header := make([]byte, 9) // 1 (Type) + 4 (ReqID) + 4 (Length)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	f := &Frame{
		Type:   header[0],
		ReqID:  binary.BigEndian.Uint32(header[1:5]),
		Length: binary.BigEndian.Uint32(header[5:9]),
	}

	if f.Length > 0 {
		f.Body = make([]byte, f.Length)
		if _, err := io.ReadFull(r, f.Body); err != nil {
			return nil, err
		}
	}

	return f, nil
}

// WriteFrame writes a Frame to the writer.
func WriteFrame(w io.Writer, f *Frame) error {
	header := make([]byte, 9)
	header[0] = f.Type
	binary.BigEndian.PutUint32(header[1:5], f.ReqID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(f.Body)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(f.Body) > 0 {
		if _, err := w.Write(f.Body); err != nil {
			return err
		}
	}
	return nil
}

// --- Request/Response Structures and Serialization ---

// ProduceRequest
type ProduceRequest struct {
	Topic string
	Key   []byte
	Value []byte
}

func (r *ProduceRequest) Marshal() []byte {
	topicBytes := []byte(r.Topic)
	buf := make([]byte, 2+len(topicBytes)+4+len(r.Key)+4+len(r.Value))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:2+len(topicBytes)], topicBytes)

	idx := 2 + len(topicBytes)
	binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(r.Key)))
	copy(buf[idx+4:idx+4+len(r.Key)], r.Key)

	idx = idx + 4 + len(r.Key)
	binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(r.Value)))
	copy(buf[idx+4:idx+4+len(r.Value)], r.Value)

	return buf
}

func UnmarshalProduceRequest(body []byte) (*ProduceRequest, error) {
	if len(body) < 2 {
		return nil, errors.New("malformed ProduceRequest")
	}
	topicLen := int(binary.BigEndian.Uint16(body[0:2]))
	if len(body) < 2+topicLen+4 {
		return nil, errors.New("malformed ProduceRequest")
	}
	topic := string(body[2 : 2+topicLen])

	idx := 2 + topicLen
	keyLen := int(binary.BigEndian.Uint32(body[idx : idx+4]))
	if len(body) < idx+4+keyLen+4 {
		return nil, errors.New("malformed ProduceRequest")
	}
	key := make([]byte, keyLen)
	copy(key, body[idx+4:idx+4+keyLen])

	idx = idx + 4 + keyLen
	valLen := int(binary.BigEndian.Uint32(body[idx : idx+4]))
	if len(body) < idx+4+valLen {
		return nil, errors.New("malformed ProduceRequest")
	}
	value := make([]byte, valLen)
	copy(value, body[idx+4:idx+4+valLen])

	return &ProduceRequest{Topic: topic, Key: key, Value: value}, nil
}

// ProduceResponse
type ProduceResponse struct {
	Offset uint64
}

func (r *ProduceResponse) Marshal() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], r.Offset)
	return buf
}

func UnmarshalProduceResponse(body []byte) (*ProduceResponse, error) {
	if len(body) < 8 {
		return nil, errors.New("malformed ProduceResponse")
	}
	return &ProduceResponse{Offset: binary.BigEndian.Uint64(body[0:8])}, nil
}

// FetchRequest
type FetchRequest struct {
	Topic             string
	Group             string
	ConsumerID        string
	MaxMessages       uint32
	AckTimeoutMs      uint32
	LongPollTimeoutMs uint32
}

func (r *FetchRequest) Marshal() []byte {
	topicBytes := []byte(r.Topic)
	groupBytes := []byte(r.Group)
	consumerBytes := []byte(r.ConsumerID)

	buf := make([]byte, 2+len(topicBytes)+2+len(groupBytes)+2+len(consumerBytes)+4+4+4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:2+len(topicBytes)], topicBytes)

	idx := 2 + len(topicBytes)
	binary.BigEndian.PutUint16(buf[idx:idx+2], uint16(len(groupBytes)))
	copy(buf[idx+2:idx+2+len(groupBytes)], groupBytes)

	idx = idx + 2 + len(groupBytes)
	binary.BigEndian.PutUint16(buf[idx:idx+2], uint16(len(consumerBytes)))
	copy(buf[idx+2:idx+2+len(consumerBytes)], consumerBytes)

	idx = idx + 2 + len(consumerBytes)
	binary.BigEndian.PutUint32(buf[idx:idx+4], r.MaxMessages)
	binary.BigEndian.PutUint32(buf[idx+4:idx+8], r.AckTimeoutMs)
	binary.BigEndian.PutUint32(buf[idx+8:idx+12], r.LongPollTimeoutMs)

	return buf
}

func UnmarshalFetchRequest(body []byte) (*FetchRequest, error) {
	if len(body) < 2 {
		return nil, errors.New("malformed FetchRequest")
	}
	topicLen := int(binary.BigEndian.Uint16(body[0:2]))
	if len(body) < 2+topicLen+2 {
		return nil, errors.New("malformed FetchRequest")
	}
	topic := string(body[2 : 2+topicLen])

	idx := 2 + topicLen
	groupLen := int(binary.BigEndian.Uint16(body[idx : idx+2]))
	if len(body) < idx+2+groupLen+2 {
		return nil, errors.New("malformed FetchRequest")
	}
	group := string(body[idx+2 : idx+2+groupLen])

	idx = idx + 2 + groupLen
	consumerLen := int(binary.BigEndian.Uint16(body[idx : idx+2]))
	if len(body) < idx+2+consumerLen+12 {
		return nil, errors.New("malformed FetchRequest")
	}
	consumerID := string(body[idx+2 : idx+2+consumerLen])

	idx = idx + 2 + consumerLen
	maxMessages := binary.BigEndian.Uint32(body[idx : idx+4])
	ackTimeout := binary.BigEndian.Uint32(body[idx+4 : idx+8])
	longPollTimeout := binary.BigEndian.Uint32(body[idx+8 : idx+12])

	return &FetchRequest{
		Topic:             topic,
		Group:             group,
		ConsumerID:        consumerID,
		MaxMessages:       maxMessages,
		AckTimeoutMs:      ackTimeout,
		LongPollTimeoutMs: longPollTimeout,
	}, nil
}

// Message represents a serialized WAL message returned in Fetch
type Message struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// FetchResponse
type FetchResponse struct {
	Messages []Message
}

func (r *FetchResponse) Marshal() []byte {
	// First calculate total size
	totalSize := 4 // count
	for _, m := range r.Messages {
		totalSize += 8 + 8 + 4 + len(m.Key) + 4 + len(m.Value)
	}

	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(r.Messages)))

	idx := 4
	for _, m := range r.Messages {
		binary.BigEndian.PutUint64(buf[idx:idx+8], m.Offset)
		binary.BigEndian.PutUint64(buf[idx+8:idx+16], uint64(m.Timestamp))
		binary.BigEndian.PutUint32(buf[idx+16:idx+20], uint32(len(m.Key)))
		copy(buf[idx+20:idx+20+len(m.Key)], m.Key)

		idx = idx + 20 + len(m.Key)
		binary.BigEndian.PutUint32(buf[idx:idx+4], uint32(len(m.Value)))
		copy(buf[idx+4:idx+4+len(m.Value)], m.Value)
		idx = idx + 4 + len(m.Value)
	}

	return buf
}

func UnmarshalFetchResponse(body []byte) (*FetchResponse, error) {
	if len(body) < 4 {
		return nil, errors.New("malformed FetchResponse")
	}
	count := int(binary.BigEndian.Uint32(body[0:4]))
	messages := make([]Message, count)

	idx := 4
	for i := 0; i < count; i++ {
		if len(body) < idx+20 {
			return nil, errors.New("malformed FetchResponse")
		}
		offset := binary.BigEndian.Uint64(body[idx : idx+8])
		timestamp := int64(binary.BigEndian.Uint64(body[idx+8 : idx+16]))
		keyLen := int(binary.BigEndian.Uint32(body[idx+16 : idx+20]))

		if len(body) < idx+20+keyLen+4 {
			return nil, errors.New("malformed FetchResponse")
		}
		key := make([]byte, keyLen)
		copy(key, body[idx+20:idx+20+keyLen])

		idx = idx + 20 + keyLen
		valLen := int(binary.BigEndian.Uint32(body[idx : idx+4]))
		if len(body) < idx+4+valLen {
			return nil, errors.New("malformed FetchResponse")
		}
		value := make([]byte, valLen)
		copy(value, body[idx+4:idx+4+valLen])

		messages[i] = Message{
			Offset:    offset,
			Timestamp: timestamp,
			Key:       key,
			Value:     value,
		}
		idx = idx + 4 + valLen
	}

	return &FetchResponse{Messages: messages}, nil
}

// AckRequest
type AckRequest struct {
	Topic  string
	Group  string
	Offset uint64
}

func (r *AckRequest) Marshal() []byte {
	topicBytes := []byte(r.Topic)
	groupBytes := []byte(r.Group)

	buf := make([]byte, 2+len(topicBytes)+2+len(groupBytes)+8)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:2+len(topicBytes)], topicBytes)

	idx := 2 + len(topicBytes)
	binary.BigEndian.PutUint16(buf[idx:idx+2], uint16(len(groupBytes)))
	copy(buf[idx+2:idx+2+len(groupBytes)], groupBytes)

	idx = idx + 2 + len(groupBytes)
	binary.BigEndian.PutUint64(buf[idx:idx+8], r.Offset)

	return buf
}

func UnmarshalAckRequest(body []byte) (*AckRequest, error) {
	if len(body) < 2 {
		return nil, errors.New("malformed AckRequest")
	}
	topicLen := int(binary.BigEndian.Uint16(body[0:2]))
	if len(body) < 2+topicLen+2 {
		return nil, errors.New("malformed AckRequest")
	}
	topic := string(body[2 : 2+topicLen])

	idx := 2 + topicLen
	groupLen := int(binary.BigEndian.Uint16(body[idx : idx+2]))
	if len(body) < idx+2+groupLen+8 {
		return nil, errors.New("malformed AckRequest")
	}
	group := string(body[idx+2 : idx+2+groupLen])

	idx = idx + 2 + groupLen
	offset := binary.BigEndian.Uint64(body[idx : idx+8])

	return &AckRequest{Topic: topic, Group: group, Offset: offset}, nil
}

// NackRequest represents negative acknowledgement
type NackRequest struct {
	Topic  string
	Group  string
	Offset uint64
}

func (r *NackRequest) Marshal() []byte {
	topicBytes := []byte(r.Topic)
	groupBytes := []byte(r.Group)

	buf := make([]byte, 2+len(topicBytes)+2+len(groupBytes)+8)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:2+len(topicBytes)], topicBytes)

	idx := 2 + len(topicBytes)
	binary.BigEndian.PutUint16(buf[idx:idx+2], uint16(len(groupBytes)))
	copy(buf[idx+2:idx+2+len(groupBytes)], groupBytes)

	idx = idx + 2 + len(groupBytes)
	binary.BigEndian.PutUint64(buf[idx:idx+8], r.Offset)

	return buf
}

func UnmarshalNackRequest(body []byte) (*NackRequest, error) {
	if len(body) < 2 {
		return nil, errors.New("malformed NackRequest")
	}
	topicLen := int(binary.BigEndian.Uint16(body[0:2]))
	if len(body) < 2+topicLen+2 {
		return nil, errors.New("malformed NackRequest")
	}
	topic := string(body[2 : 2+topicLen])

	idx := 2 + topicLen
	groupLen := int(binary.BigEndian.Uint16(body[idx : idx+2]))
	if len(body) < idx+2+groupLen+8 {
		return nil, errors.New("malformed NackRequest")
	}
	group := string(body[idx+2 : idx+2+groupLen])

	idx = idx + 2 + groupLen
	offset := binary.BigEndian.Uint64(body[idx : idx+8])

	return &NackRequest{Topic: topic, Group: group, Offset: offset}, nil
}

// CreateTopicRequest
type CreateTopicRequest struct {
	Topic string
}

func (r *CreateTopicRequest) Marshal() []byte {
	topicBytes := []byte(r.Topic)
	buf := make([]byte, 2+len(topicBytes))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:], topicBytes)
	return buf
}

func UnmarshalCreateTopicRequest(body []byte) (*CreateTopicRequest, error) {
	if len(body) < 2 {
		return nil, errors.New("malformed CreateTopicRequest")
	}
	topicLen := int(binary.BigEndian.Uint16(body[0:2]))
	if len(body) < 2+topicLen {
		return nil, errors.New("malformed CreateTopicRequest")
	}
	topic := string(body[2 : 2+topicLen])
	return &CreateTopicRequest{Topic: topic}, nil
}

// ErrorResponse
type ErrorResponse struct {
	Code    uint16
	Message string
}

func (r *ErrorResponse) Marshal() []byte {
	msgBytes := []byte(r.Message)
	buf := make([]byte, 2+2+len(msgBytes))
	binary.BigEndian.PutUint16(buf[0:2], r.Code)
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(msgBytes)))
	copy(buf[4:], msgBytes)
	return buf
}

func UnmarshalErrorResponse(body []byte) (*ErrorResponse, error) {
	if len(body) < 4 {
		return nil, errors.New("malformed ErrorResponse")
	}
	code := binary.BigEndian.Uint16(body[0:2])
	msgLen := int(binary.BigEndian.Uint16(body[2:4]))
	if len(body) < 4+msgLen {
		return nil, errors.New("malformed ErrorResponse")
	}
	message := string(body[4 : 4+msgLen])
	return &ErrorResponse{Code: code, Message: message}, nil
}
