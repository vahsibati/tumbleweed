package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MagicByte byte = 0x54 // 'T' for Tumbleweed
)

var (
	ErrRecordCorrupt  = errors.New("wal: record checksum mismatch, data corruption detected")
	ErrOffsetNotFound = errors.New("wal: offset not found")
)

// Record represents a single message in the WAL.
type Record struct {
	Magic     byte
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	Checksum  uint32
}

// Segment represents a log segment file and its index.
type Segment struct {
	BaseOffset uint64
	logPath    string
	idxPath    string
	logFile    *os.File
	idxFile    *os.File
	size       int64
	idxSize    int64
	mu         sync.Mutex
}

// WAL represents a write-ahead log for a single topic.
type WAL struct {
	dir             string
	syncEveryWrite  bool
	syncInterval    time.Duration
	maxSegmentBytes int64
	segments        []*Segment
	active          *Segment
	nextOffset      uint64
	closed          bool
	mu              sync.RWMutex
	flushChan       chan struct{}
}

// NewWAL opens or creates a WAL in the specified directory.
func NewWAL(dir string, syncEveryWrite bool, syncInterval time.Duration, maxSegmentBytes int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	w := &WAL{
		dir:             dir,
		syncEveryWrite:  syncEveryWrite,
		syncInterval:    syncInterval,
		maxSegmentBytes: maxSegmentBytes,
		flushChan:       make(chan struct{}, 1),
	}

	if err := w.loadSegments(); err != nil {
		return nil, fmt.Errorf("failed to load segments: %w", err)
	}

	if !w.syncEveryWrite && w.syncInterval > 0 {
		go w.flushLoop()
	}

	return w, nil
}

// loadSegments reads the WAL directory and loads/rebuilds all segments.
func (w *WAL) loadSegments() error {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if strings.HasSuffix(f.Name(), ".log") {
			name := strings.TrimSuffix(f.Name(), ".log")
			val, err := strconv.ParseUint(name, 10, 64)
			if err != nil {
				continue
			}
			baseOffsets = append(baseOffsets, val)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, bo := range baseOffsets {
		seg, err := openSegment(w.dir, bo)
		if err != nil {
			return err
		}
		if err := w.verifyOrRebuildIndex(seg); err != nil {
			return err
		}
		w.segments = append(w.segments, seg)
	}

	if len(w.segments) == 0 {
		seg, err := openSegment(w.dir, 0)
		if err != nil {
			return err
		}
		w.segments = append(w.segments, seg)
	}

	w.active = w.segments[len(w.segments)-1]

	// Determine nextOffset
	if err := w.recoverNextOffset(); err != nil {
		return err
	}

	return nil
}

func openSegment(dir string, baseOffset uint64) (*Segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOffset)
	idxName := fmt.Sprintf("%020d.index", baseOffset)

	logPath := filepath.Join(dir, logName)
	idxPath := filepath.Join(dir, idxName)

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	logStat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		idxFile.Close()
		return nil, err
	}

	idxStat, err := idxFile.Stat()
	if err != nil {
		logFile.Close()
		idxFile.Close()
		return nil, err
	}

	return &Segment{
		BaseOffset: baseOffset,
		logPath:    logPath,
		idxPath:    idxPath,
		logFile:    logFile,
		idxFile:    idxFile,
		size:       logStat.Size(),
		idxSize:    idxStat.Size(),
	}, nil
}

// verifyOrRebuildIndex scans the log segment and ensures the index is valid.
// If index is corrupt or incomplete, it rebuilds it from the log file.
func (w *WAL) verifyOrRebuildIndex(seg *Segment) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	// Parse records in log file and check if index matches.
	// Index contains uint64 offsets. Each offset is 8 bytes.
	// Expected index size = number of records * 8.

	// Fast validation: try parsing records and match their positions.
	if _, err := seg.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var recordsCount int64
	var offsetBuf [8]byte

	// We scan the log file.
	var logPos int64 = 0
	rebuildNeeded := false

	for {
		if logPos >= seg.size {
			break
		}

		// Read magic byte
		var magic [1]byte
		if _, err := seg.logFile.Read(magic[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if magic[0] != MagicByte {
			// Corrupt segment tail or invalid magic, truncate log file to this position
			rebuildNeeded = true
			break
		}

		// Read header: Offset (8), Timestamp (8), KeyLen (4), ValLen (4)
		headerBuf := make([]byte, 24)
		if _, err := io.ReadFull(seg.logFile, headerBuf); err != nil {
			rebuildNeeded = true
			break
		}

		keyLen := binary.BigEndian.Uint32(headerBuf[16:20])
		valLen := binary.BigEndian.Uint32(headerBuf[20:24])

		// Skip Key, Value, Checksum (4)
		recordSize := int64(1 + 24 + keyLen + valLen + 4)

		// Verify if the index matches this position
		var idxPos int64 = recordsCount * 8
		if idxPos+8 > seg.idxSize {
			rebuildNeeded = true
		} else {
			if _, err := seg.idxFile.Seek(idxPos, io.SeekStart); err != nil {
				return err
			}
			if _, err := io.ReadFull(seg.idxFile, offsetBuf[:]); err != nil {
				return err
			}
			storedPos := int64(binary.BigEndian.Uint64(offsetBuf[:]))
			if storedPos != logPos {
				rebuildNeeded = true
			}
		}

		// Seek forward in log
		logPos += recordSize
		if _, err := seg.logFile.Seek(logPos, io.SeekStart); err != nil {
			return err
		}
		recordsCount++
	}

	// Truncate log to last clean position if corrupt tail detected
	if logPos < seg.size {
		if err := seg.logFile.Truncate(logPos); err != nil {
			return err
		}
		seg.size = logPos
	}

	if rebuildNeeded || seg.idxSize != recordsCount*8 {
		// Rebuild index
		if err := seg.idxFile.Truncate(0); err != nil {
			return err
		}
		if _, err := seg.idxFile.Seek(0, io.SeekStart); err != nil {
			return err
		}

		if _, err := seg.logFile.Seek(0, io.SeekStart); err != nil {
			return err
		}

		var scanPos int64 = 0
		for scanPos < seg.size {
			headerBuf := make([]byte, 25) // Magic (1) + Header (24)
			if _, err := io.ReadFull(seg.logFile, headerBuf); err != nil {
				return err
			}
			keyLen := binary.BigEndian.Uint32(headerBuf[17:21])
			valLen := binary.BigEndian.Uint32(headerBuf[21:25])

			// Write position to index
			binary.BigEndian.PutUint64(offsetBuf[:], uint64(scanPos))
			if _, err := seg.idxFile.Write(offsetBuf[:]); err != nil {
				return err
			}

			recordSize := int64(1 + 24 + keyLen + valLen + 4)
			scanPos += recordSize
			if _, err := seg.logFile.Seek(scanPos, io.SeekStart); err != nil {
				return err
			}
		}

		seg.idxSize = recordsCount * 8
		if err := seg.idxFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// recoverNextOffset calculates the next offset to write to the WAL.
func (w *WAL) recoverNextOffset() error {
	w.active.mu.Lock()
	defer w.active.mu.Unlock()

	if w.active.idxSize == 0 {
		w.nextOffset = w.active.BaseOffset
		return nil
	}

	// Read last entry in active index
	lastIdxPos := w.active.idxSize - 8
	if _, err := w.active.idxFile.Seek(lastIdxPos, io.SeekStart); err != nil {
		return err
	}

	var posBuf [8]byte
	if _, err := io.ReadFull(w.active.idxFile, posBuf[:]); err != nil {
		return err
	}
	lastLogPos := int64(binary.BigEndian.Uint64(posBuf[:]))

	// Read offset from last record in active log
	if _, err := w.active.logFile.Seek(lastLogPos+1, io.SeekStart); err != nil { // skip magic
		return err
	}

	var offsetBuf [8]byte
	if _, err := io.ReadFull(w.active.logFile, offsetBuf[:]); err != nil {
		return err
	}

	lastOffset := binary.BigEndian.Uint64(offsetBuf[:])
	w.nextOffset = lastOffset + 1

	return nil
}

// Append appends a key-value record to the active segment.
func (w *WAL) Append(key, value []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, errors.New("wal: closed")
	}

	// Check if active segment has exceeded max size. If so, roll.
	if w.active.size >= w.maxSegmentBytes {
		if err := w.roll(); err != nil {
			return 0, err
		}
	}

	offset := w.nextOffset
	timestamp := time.Now().UnixNano()

	w.active.mu.Lock()
	defer w.active.mu.Unlock()

	// Write record to log
	// Record size: Magic (1) + Offset (8) + Timestamp (8) + KeyLen (4) + ValLen (4) + Key (N) + Value (M) + Checksum (4)
	buf := make([]byte, 1+8+8+4+4+len(key)+len(value)+4)
	buf[0] = MagicByte
	binary.BigEndian.PutUint64(buf[1:9], offset)
	binary.BigEndian.PutUint64(buf[9:17], uint64(timestamp))
	binary.BigEndian.PutUint32(buf[17:21], uint32(len(key)))
	binary.BigEndian.PutUint32(buf[21:25], uint32(len(value)))
	copy(buf[25:25+len(key)], key)
	copy(buf[25+len(key):25+len(key)+len(value)], value)

	crcIdx := 25 + len(key) + len(value)
	checksum := crc32.ChecksumIEEE(buf[0:crcIdx])
	binary.BigEndian.PutUint32(buf[crcIdx:crcIdx+4], checksum)

	// Write to log file
	logPos := w.active.size
	if _, err := w.active.logFile.Seek(logPos, io.SeekStart); err != nil {
		return 0, err
	}
	if _, err := w.active.logFile.Write(buf); err != nil {
		return 0, err
	}

	// Write position to index file
	idxPos := w.active.idxSize
	if _, err := w.active.idxFile.Seek(idxPos, io.SeekStart); err != nil {
		return 0, err
	}
	var posBuf [8]byte
	binary.BigEndian.PutUint64(posBuf[:], uint64(logPos))
	if _, err := w.active.idxFile.Write(posBuf[:]); err != nil {
		return 0, err
	}

	// Update sizes
	w.active.size += int64(len(buf))
	w.active.idxSize += 8

	w.nextOffset++

	if w.syncEveryWrite {
		if err := w.active.logFile.Sync(); err != nil {
			return 0, err
		}
		if err := w.active.idxFile.Sync(); err != nil {
			return 0, err
		}
	} else {
		// Signal background flush loop
		select {
		case w.flushChan <- struct{}{}:
		default:
		}
	}

	return offset, nil
}

// roll creates a new active segment starting at nextOffset.
func (w *WAL) roll() error {
	// Active lock is not held by w.mu, but active is being swapped.
	// Close files of old active segment.
	w.active.mu.Lock()
	if err := w.active.logFile.Sync(); err != nil {
		w.active.mu.Unlock()
		return err
	}
	if err := w.active.idxFile.Sync(); err != nil {
		w.active.mu.Unlock()
		return err
	}
	w.active.mu.Unlock()

	// Open new segment
	newSeg, err := openSegment(w.dir, w.nextOffset)
	if err != nil {
		return err
	}

	w.segments = append(w.segments, newSeg)
	w.active = newSeg
	return nil
}

// Read reads a single record at the given offset.
func (w *WAL) Read(offset uint64) (*Record, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if offset >= w.nextOffset {
		return nil, ErrOffsetNotFound
	}

	// Find the segment containing the offset
	seg := w.findSegment(offset)
	if seg == nil {
		return nil, ErrOffsetNotFound
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()

	// Calculate index offset
	idxOffset := int64(offset-seg.BaseOffset) * 8
	if idxOffset+8 > seg.idxSize {
		return nil, ErrOffsetNotFound
	}

	// Read index to get log file position
	if _, err := seg.idxFile.Seek(idxOffset, io.SeekStart); err != nil {
		return nil, err
	}
	var posBuf [8]byte
	if _, err := io.ReadFull(seg.idxFile, posBuf[:]); err != nil {
		return nil, err
	}
	logPos := int64(binary.BigEndian.Uint64(posBuf[:]))

	// Read record from log file
	if _, err := seg.logFile.Seek(logPos, io.SeekStart); err != nil {
		return nil, err
	}

	var magic [1]byte
	if _, err := seg.logFile.Read(magic[:]); err != nil {
		return nil, err
	}
	if magic[0] != MagicByte {
		return nil, ErrRecordCorrupt
	}

	headerBuf := make([]byte, 24)
	if _, err := io.ReadFull(seg.logFile, headerBuf); err != nil {
		return nil, err
	}

	recOffset := binary.BigEndian.Uint64(headerBuf[0:8])
	timestamp := int64(binary.BigEndian.Uint64(headerBuf[8:16]))
	keyLen := binary.BigEndian.Uint32(headerBuf[16:20])
	valLen := binary.BigEndian.Uint32(headerBuf[20:24])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(seg.logFile, key); err != nil {
		return nil, err
	}

	value := make([]byte, valLen)
	if _, err := io.ReadFull(seg.logFile, value); err != nil {
		return nil, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(seg.logFile, crcBuf[:]); err != nil {
		return nil, err
	}
	checksum := binary.BigEndian.Uint32(crcBuf[:])

	// Verify CRC
	totalLen := 1 + 24 + len(key) + len(value)
	bufForCrc := make([]byte, totalLen)
	bufForCrc[0] = MagicByte
	binary.BigEndian.PutUint64(bufForCrc[1:9], recOffset)
	binary.BigEndian.PutUint64(bufForCrc[9:17], uint64(timestamp))
	binary.BigEndian.PutUint32(bufForCrc[17:21], keyLen)
	binary.BigEndian.PutUint32(bufForCrc[21:25], valLen)
	copy(bufForCrc[25:25+len(key)], key)
	copy(bufForCrc[25+len(key):], value)

	expectedCrc := crc32.ChecksumIEEE(bufForCrc)
	if checksum != expectedCrc {
		return nil, ErrRecordCorrupt
	}

	return &Record{
		Magic:     MagicByte,
		Offset:    recOffset,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
		Checksum:  checksum,
	}, nil
}

// findSegment performs binary search to find the segment that contains the offset.
func (w *WAL) findSegment(offset uint64) *Segment {
	// The segments are sorted by BaseOffset.
	// We want to find the segment whose BaseOffset is the largest possible but <= offset.
	idx := sort.Search(len(w.segments), func(i int) bool {
		return w.segments[i].BaseOffset > offset
	})
	if idx > 0 {
		return w.segments[idx-1]
	}
	if len(w.segments) > 0 && w.segments[0].BaseOffset <= offset {
		return w.segments[0]
	}
	return nil
}

// NextOffset returns the next offset to be written.
func (w *WAL) NextOffset() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.nextOffset
}

// Flush forces syncing of active segment.
func (w *WAL) Flush() error {
	w.mu.RLock()
	active := w.active
	w.mu.RUnlock()

	active.mu.Lock()
	defer active.mu.Unlock()

	if err := active.logFile.Sync(); err != nil {
		return err
	}
	return active.idxFile.Sync()
}

// flushLoop runs in background if async sync is enabled.
func (w *WAL) flushLoop() {
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-w.flushChan:
		}

		w.mu.RLock()
		if w.closed {
			w.mu.RUnlock()
			return
		}
		w.mu.RUnlock()

		_ = w.Flush()
	}
}

// Close closes all open segment files.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	// Close channels
	close(w.flushChan)

	var lastErr error
	for _, seg := range w.segments {
		seg.mu.Lock()
		if err := seg.logFile.Sync(); err != nil {
			lastErr = err
		}
		if err := seg.idxFile.Sync(); err != nil {
			lastErr = err
		}
		if err := seg.logFile.Close(); err != nil {
			lastErr = err
		}
		if err := seg.idxFile.Close(); err != nil {
			lastErr = err
		}
		seg.mu.Unlock()
	}

	return lastErr
}
