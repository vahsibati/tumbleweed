package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"tumbleweed/pkg/config"
	"tumbleweed/pkg/protocol"
	"tumbleweed/pkg/wal"
)

var (
	ErrTopicNotFound = errors.New("broker: topic not found")
	ErrGroupNotFound = errors.New("broker: consumer group not found")
	ErrLeaseNotFound = errors.New("broker: lease not found or already acknowledged")
)

// LeaseState tracks the active lease of a message delivered to a consumer.
type LeaseState struct {
	Offset       uint64    `json:"offset"`
	ConsumerID   string    `json:"consumer_id"`
	Expiry       time.Time `json:"expiry"`
	Redeliveries int       `json:"redeliveries"`
	Acked        bool      `json:"acked"`
}

// GroupState represents the persisted state of a consumer group.
type GroupState struct {
	CommittedOffset uint64 `json:"committed_offset"`
}

// ConsumerGroup tracks offset and active leases.
type ConsumerGroup struct {
	Name            string
	TopicName       string
	CommittedOffset uint64
	Leases          map[uint64]*LeaseState
	statePath       string
	mu              sync.Mutex
	broker          *Broker
}

// PendingFetch represents a waiting consumer (long polling).
type PendingFetch struct {
	ConsumerID  string
	MaxMessages uint32
	AckTimeout  time.Duration
	RespChan    chan []protocol.Message
	Expiry      time.Time
}

// Topic coordinates the WAL and consumer groups.
type Topic struct {
	Name           string
	wal            *wal.WAL
	groups         map[string]*ConsumerGroup
	pendingFetches map[string][]*PendingFetch // group -> list
	dir            string
	mu             sync.RWMutex
	broker         *Broker
}

// Broker is the primary container for all topics.
type Broker struct {
	cfg       *config.Config
	topics    map[string]*Topic
	mu        sync.RWMutex
	closed    bool
	closeChan chan struct{}
}

// NewBroker initializes the broker.
func NewBroker(cfg *config.Config) (*Broker, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}

	b := &Broker{
		cfg:       cfg,
		topics:    make(map[string]*Topic),
		closeChan: make(chan struct{}),
	}

	if err := b.loadTopics(); err != nil {
		return nil, err
	}

	go b.leaseExpiryLoop()

	return b, nil
}

func (b *Broker) loadTopics() error {
	topicsDir := filepath.Join(b.cfg.DataDir, "topics")
	if err := os.MkdirAll(topicsDir, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(topicsDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			_, err := b.CreateTopic(entry.Name())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateTopic explicitly creates a topic and opens its WAL.
func (b *Broker) CreateTopic(name string) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, exists := b.topics[name]; exists {
		return t, nil
	}

	topicDir := filepath.Join(b.cfg.DataDir, "topics", name)
	walDir := filepath.Join(topicDir, "wal")

	walEngine, err := wal.NewWAL(walDir, b.cfg.SyncEveryWrite, b.cfg.SyncInterval, b.cfg.MaxSegmentBytes)
	if err != nil {
		return nil, err
	}

	t := &Topic{
		Name:           name,
		wal:            walEngine,
		groups:         make(map[string]*ConsumerGroup),
		pendingFetches: make(map[string][]*PendingFetch),
		dir:            topicDir,
		broker:         b,
	}

	if err := t.loadGroups(); err != nil {
		walEngine.Close()
		return nil, err
	}

	b.topics[name] = t
	return t, nil
}

func (t *Topic) loadGroups() error {
	groupsDir := filepath.Join(t.dir, "groups")
	if err := os.MkdirAll(groupsDir, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(groupsDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".state" {
			groupName := strings.TrimSuffix(entry.Name(), ".state")
			statePath := filepath.Join(groupsDir, entry.Name())

			file, err := os.Open(statePath)
			if err != nil {
				continue
			}

			var state GroupState
			err = json.NewDecoder(file).Decode(&state)
			file.Close()
			if err != nil {
				continue
			}

			t.groups[groupName] = &ConsumerGroup{
				Name:            groupName,
				TopicName:       t.Name,
				CommittedOffset: state.CommittedOffset,
				Leases:          make(map[uint64]*LeaseState),
				statePath:       statePath,
				broker:          t.broker,
			}
		}
	}

	return nil
}

func (t *Topic) getOrCreateGroup(name string) *ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	g, exists := t.groups[name]
	if !exists {
		statePath := filepath.Join(t.dir, "groups", name+".state")
		g = &ConsumerGroup{
			Name:            name,
			TopicName:       t.Name,
			CommittedOffset: 0,
			Leases:          make(map[uint64]*LeaseState),
			statePath:       statePath,
			broker:          t.broker,
		}
		t.groups[name] = g
		_ = g.persistState() // Ignore error on initial save
	}
	return g
}

func (g *ConsumerGroup) persistState() error {
	dir := filepath.Dir(g.statePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpPath := g.statePath + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	state := GroupState{CommittedOffset: g.CommittedOffset}
	if err := json.NewEncoder(file).Encode(state); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	file.Close()

	return os.Rename(tmpPath, g.statePath)
}

// Produce publishes a message to a topic and dispatches it to waiting consumers.
func (b *Broker) Produce(topicName string, key, value []byte) (uint64, error) {
	t, err := b.CreateTopic(topicName)
	if err != nil {
		return 0, err
	}

	offset, err := t.wal.Append(key, value)
	if err != nil {
		return 0, err
	}

	// Read appended record for dispatch
	rec, err := t.wal.Read(offset)
	if err != nil {
		return offset, nil // Written but failed to dispatch immediately, clients will poll
	}

	msg := protocol.Message{
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp,
		Key:       rec.Key,
		Value:     rec.Value,
	}

	t.dispatchMessage(msg)

	return offset, nil
}

func (t *Topic) dispatchMessage(msg protocol.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for gName, fetches := range t.pendingFetches {
		if len(fetches) == 0 {
			continue
		}

		g := t.getOrCreateGroup(gName)
		g.mu.Lock()

		// Double-check if we can dispatch this message to the first pending fetcher.
		// Next available offset must be this message's offset.
		var nextExpectedOffset uint64 = g.CommittedOffset
		for o := range g.Leases {
			if o >= nextExpectedOffset {
				nextExpectedOffset = o + 1
			}
		}

		if msg.Offset == nextExpectedOffset {
			// Pull the first pending fetcher
			pf := fetches[0]
			t.pendingFetches[gName] = fetches[1:]

			// Create lease
			g.Leases[msg.Offset] = &LeaseState{
				Offset:       msg.Offset,
				ConsumerID:   pf.ConsumerID,
				Expiry:       time.Now().Add(pf.AckTimeout),
				Redeliveries: 0,
				Acked:        false,
			}

			// Send message
			select {
			case pf.RespChan <- []protocol.Message{msg}:
			default:
				// If channel is blocked, discard lease and put back in queue?
				// To keep it simple, if consumer disconnected, we'll let it time out
			}
			g.mu.Unlock()
			continue
		}

		g.mu.Unlock()
	}
}

// Fetch retrieves messages or waits for them.
func (b *Broker) Fetch(req *protocol.FetchRequest) ([]protocol.Message, error) {
	t, err := b.CreateTopic(req.Topic)
	if err != nil {
		return nil, err
	}

	g := t.getOrCreateGroup(req.Group)

	ackTimeout := time.Duration(req.AckTimeoutMs) * time.Millisecond
	if ackTimeout == 0 {
		ackTimeout = b.cfg.DefaultLeaseTimeout
	}

	g.mu.Lock()
	messages := g.fetchAvailable(t, req.ConsumerID, req.MaxMessages, ackTimeout)
	g.mu.Unlock()

	if len(messages) > 0 {
		return messages, nil
	}

	// Long Poll
	if req.LongPollTimeoutMs == 0 {
		return nil, nil
	}

	respChan := make(chan []protocol.Message, 1)
	pf := &PendingFetch{
		ConsumerID:  req.ConsumerID,
		MaxMessages: req.MaxMessages,
		AckTimeout:  ackTimeout,
		RespChan:    respChan,
		Expiry:      time.Now().Add(time.Duration(req.LongPollTimeoutMs) * time.Millisecond),
	}

	t.mu.Lock()
	t.pendingFetches[req.Group] = append(t.pendingFetches[req.Group], pf)
	t.mu.Unlock()

	// Wait for message or timeout
	select {
	case msgs := <-respChan:
		return msgs, nil
	case <-time.After(time.Duration(req.LongPollTimeoutMs) * time.Millisecond):
		// Clean up pending fetch
		t.mu.Lock()
		fetches := t.pendingFetches[req.Group]
		for i, f := range fetches {
			if f == pf {
				t.pendingFetches[req.Group] = append(fetches[:i], fetches[i+1:]...)
				break
			}
		}
		t.mu.Unlock()
		return nil, nil
	}
}

// fetchAvailable fetches available messages (expired leases or new offsets).
// MUST hold g.mu.
func (g *ConsumerGroup) fetchAvailable(t *Topic, consumerID string, maxCount uint32, ackTimeout time.Duration) []protocol.Message {
	var msgs []protocol.Message

	// 1. Check for expired leases
	var expiredOffsets []uint64
	for o, lease := range g.Leases {
		if !lease.Acked && time.Now().After(lease.Expiry) {
			expiredOffsets = append(expiredOffsets, o)
		}
	}

	// Sort expired offsets to deliver in order
	// Note: We can use a simpler sort.Slice here
	for i := 0; i < len(expiredOffsets); i++ {
		for j := i + 1; j < len(expiredOffsets); j++ {
			if expiredOffsets[i] > expiredOffsets[j] {
				expiredOffsets[i], expiredOffsets[j] = expiredOffsets[j], expiredOffsets[i]
			}
		}
	}

	for _, offset := range expiredOffsets {
		if uint32(len(msgs)) >= maxCount {
			break
		}

		lease := g.Leases[offset]
		if lease.Redeliveries >= g.broker.cfg.MaxRedeliveries {
			// Exceeded max redeliveries, send to DLQ
			rec, err := t.wal.Read(offset)
			if err == nil {
				dlqTopic := fmt.Sprintf("%s-dlq", t.Name)
				_, _ = g.broker.Produce(dlqTopic, rec.Key, rec.Value)
			}
			// Mark as acked in this group to advance offset
			lease.Acked = true
			g.slideWindow()
			continue
		}

		rec, err := t.wal.Read(offset)
		if err != nil {
			continue
		}

		lease.ConsumerID = consumerID
		lease.Expiry = time.Now().Add(ackTimeout)
		lease.Redeliveries++

		msgs = append(msgs, protocol.Message{
			Offset:    rec.Offset,
			Timestamp: rec.Timestamp,
			Key:       rec.Key,
			Value:     rec.Value,
		})
	}

	// 2. Fetch new messages from WAL
	nextOffset := g.CommittedOffset
	for o := range g.Leases {
		if o >= nextOffset {
			nextOffset = o + 1
		}
	}

	walNextOffset := t.wal.NextOffset()
	for nextOffset < walNextOffset && uint32(len(msgs)) < maxCount {
		rec, err := t.wal.Read(nextOffset)
		if err != nil {
			break
		}

		g.Leases[nextOffset] = &LeaseState{
			Offset:       nextOffset,
			ConsumerID:   consumerID,
			Expiry:       time.Now().Add(ackTimeout),
			Redeliveries: 0,
			Acked:        false,
		}

		msgs = append(msgs, protocol.Message{
			Offset:    rec.Offset,
			Timestamp: rec.Timestamp,
			Key:       rec.Key,
			Value:     rec.Value,
		})
		nextOffset++
	}

	return msgs
}

// Ack acknowledges a message offset.
func (b *Broker) Ack(topicName, groupName string, offset uint64) error {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return ErrTopicNotFound
	}

	g := t.getOrCreateGroup(groupName)
	g.mu.Lock()
	defer g.mu.Unlock()

	if offset < g.CommittedOffset {
		return nil // Already committed
	}

	lease, exists := g.Leases[offset]
	if !exists {
		return ErrLeaseNotFound
	}

	lease.Acked = true
	g.slideWindow()

	return nil
}

// Nack rejects a message, making it immediately available for redelivery.
func (b *Broker) Nack(topicName, groupName string, offset uint64) error {
	b.mu.RLock()
	t, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return ErrTopicNotFound
	}

	g := t.getOrCreateGroup(groupName)
	g.mu.Lock()
	defer g.mu.Unlock()

	lease, exists := g.Leases[offset]
	if !exists || lease.Acked {
		return ErrLeaseNotFound
	}

	// Immediately expire lease
	lease.Expiry = time.Now()

	// Proactively check if there are pending fetches we can immediately dispatch to
	t.mu.Lock()
	fetches, exists := t.pendingFetches[groupName]
	if exists && len(fetches) > 0 {
		pf := fetches[0]
		t.pendingFetches[groupName] = fetches[1:]
		t.mu.Unlock()

		// Re-lease immediately
		lease.ConsumerID = pf.ConsumerID
		lease.Expiry = time.Now().Add(pf.AckTimeout)
		lease.Redeliveries++

		rec, err := t.wal.Read(offset)
		if err == nil {
			select {
			case pf.RespChan <- []protocol.Message{{
				Offset:    rec.Offset,
				Timestamp: rec.Timestamp,
				Key:       rec.Key,
				Value:     rec.Value,
			}}:
			default:
			}
		}
	} else {
		t.mu.Unlock()
	}

	return nil
}

// slideWindow advances the committed offset as far as possible.
// MUST hold g.mu.
func (g *ConsumerGroup) slideWindow() {
	advanced := false
	for {
		lease, exists := g.Leases[g.CommittedOffset]
		if exists && lease.Acked {
			delete(g.Leases, g.CommittedOffset)
			g.CommittedOffset++
			advanced = true
		} else {
			break
		}
	}

	if advanced {
		_ = g.persistState()
	}
}

// leaseExpiryLoop checks for expired leases and triggers redelivery if consumers are waiting.
func (b *Broker) leaseExpiryLoop() {
	ticker := time.NewTicker(b.cfg.LeaseCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mu.RLock()
			if b.closed {
				b.mu.RUnlock()
				return
			}
			// Copy topics map reference to avoid holding read lock too long
			topics := make([]*Topic, 0, len(b.topics))
			for _, t := range b.topics {
				topics = append(topics, t)
			}
			b.mu.RUnlock()

			for _, t := range topics {
				t.checkExpiredLeases()
			}
		case <-b.closeChan:
			return
		}
	}
}

func (t *Topic) checkExpiredLeases() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for gName, fetches := range t.pendingFetches {
		if len(fetches) == 0 {
			continue
		}

		g, exists := t.groups[gName]
		if !exists {
			continue
		}

		g.mu.Lock()

		// Find first expired lease
		var expiredOffset uint64
		var found bool
		for o, lease := range g.Leases {
			if !lease.Acked && time.Now().After(lease.Expiry) {
				if !found || o < expiredOffset {
					expiredOffset = o
					found = true
				}
			}
		}

		if found {
			pf := fetches[0]
			t.pendingFetches[gName] = fetches[1:]

			lease := g.Leases[expiredOffset]

			if lease.Redeliveries >= t.broker.cfg.MaxRedeliveries {
				// Send to DLQ
				rec, err := t.wal.Read(expiredOffset)
				if err == nil {
					dlqTopic := fmt.Sprintf("%s-dlq", t.Name)
					_, _ = t.broker.Produce(dlqTopic, rec.Key, rec.Value)
				}
				lease.Acked = true
				g.slideWindow()
				g.mu.Unlock()
				continue
			}

			rec, err := t.wal.Read(expiredOffset)
			if err == nil {
				lease.ConsumerID = pf.ConsumerID
				lease.Expiry = time.Now().Add(pf.AckTimeout)
				lease.Redeliveries++

				select {
				case pf.RespChan <- []protocol.Message{{
					Offset:    rec.Offset,
					Timestamp: rec.Timestamp,
					Key:       rec.Key,
					Value:     rec.Value,
				}}:
				default:
				}
			}
		}
		g.mu.Unlock()
	}
}

// Close closes the broker.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.closeChan)
	b.mu.Unlock()

	var lastErr error
	b.mu.RLock()
	for _, t := range b.topics {
		t.mu.Lock()
		if err := t.wal.Close(); err != nil {
			lastErr = err
		}
		t.mu.Unlock()
	}
	b.mu.RUnlock()

	return lastErr
}
