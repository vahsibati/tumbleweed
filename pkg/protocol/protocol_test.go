package protocol

import (
	"bytes"
	"reflect"
	"testing"
)

func TestFrameReadWrite(t *testing.T) {
	orig := &Frame{
		Type:   TypeReqProduce,
		ReqID:  42,
		Length: 5,
		Body:   []byte("hello"),
	}

	var buf bytes.Buffer
	err := WriteFrame(&buf, orig)
	if err != nil {
		t.Fatalf("failed to write frame: %v", err)
	}

	read, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("failed to read frame: %v", err)
	}

	if read.Type != orig.Type {
		t.Errorf("expected type %v, got %v", orig.Type, read.Type)
	}
	if read.ReqID != orig.ReqID {
		t.Errorf("expected ReqID %v, got %v", orig.ReqID, read.ReqID)
	}
	if read.Length != orig.Length {
		t.Errorf("expected Length %v, got %v", orig.Length, read.Length)
	}
	if !bytes.Equal(read.Body, orig.Body) {
		t.Errorf("expected Body %v, got %v", orig.Body, read.Body)
	}
}

func TestProduceRequestMarshalUnmarshal(t *testing.T) {
	req := &ProduceRequest{
		Topic: "test-topic",
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	data := req.Marshal()
	unmarshaled, err := UnmarshalProduceRequest(data)
	if err != nil {
		t.Fatalf("failed to unmarshal produce request: %v", err)
	}

	if !reflect.DeepEqual(req, unmarshaled) {
		t.Errorf("expected %+v, got %+v", req, unmarshaled)
	}
}

func TestFetchRequestMarshalUnmarshal(t *testing.T) {
	req := &FetchRequest{
		Topic:             "t1",
		Group:             "g1",
		ConsumerID:        "c1",
		MaxMessages:       10,
		AckTimeoutMs:      30000,
		LongPollTimeoutMs: 15000,
	}

	data := req.Marshal()
	unmarshaled, err := UnmarshalFetchRequest(data)
	if err != nil {
		t.Fatalf("failed to unmarshal fetch request: %v", err)
	}

	if !reflect.DeepEqual(req, unmarshaled) {
		t.Errorf("expected %+v, got %+v", req, unmarshaled)
	}
}

func TestFetchResponseMarshalUnmarshal(t *testing.T) {
	resp := &FetchResponse{
		Messages: []Message{
			{Offset: 1, Timestamp: 1000, Key: []byte("k1"), Value: []byte("v1")},
			{Offset: 2, Timestamp: 2000, Key: nil, Value: []byte("v2")},
		},
	}

	data := resp.Marshal()
	unmarshaled, err := UnmarshalFetchResponse(data)
	if err != nil {
		t.Fatalf("failed to unmarshal fetch response: %v", err)
	}

	if len(unmarshaled.Messages) != len(resp.Messages) {
		t.Fatalf("expected %d messages, got %d", len(resp.Messages), len(unmarshaled.Messages))
	}

	for i := range resp.Messages {
		expected := resp.Messages[i]
		got := unmarshaled.Messages[i]
		if expected.Offset != got.Offset || expected.Timestamp != got.Timestamp ||
			!bytes.Equal(expected.Key, got.Key) || !bytes.Equal(expected.Value, got.Value) {
			t.Errorf("msg %d mismatch: expected %+v, got %+v", i, expected, got)
		}
	}
}

func TestListTopicsResponseMarshalUnmarshal(t *testing.T) {
	resp := &ListTopicsResponse{
		Topics: []string{"topic-a", "topic-b", "another-one"},
	}

	data := resp.Marshal()
	unmarshaled, err := UnmarshalListTopicsResponse(data)
	if err != nil {
		t.Fatalf("failed to unmarshal list topics response: %v", err)
	}

	if !reflect.DeepEqual(resp.Topics, unmarshaled.Topics) {
		t.Errorf("expected topics %v, got %v", resp.Topics, unmarshaled.Topics)
	}
}

