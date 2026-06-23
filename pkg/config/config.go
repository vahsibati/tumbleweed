package config

import (
	"time"
)

// Config holds all configuration parameters for the Tumbleweed broker.
type Config struct {
	// DataDir is the root directory where topics and metadata will be stored.
	DataDir string

	// BindAddr is the TCP address the broker will listen on (e.g. ":8765").
	BindAddr string

	// SyncEveryWrite forces an fsync to disk on every message published.
	SyncEveryWrite bool

	// SyncInterval is the frequency of async flushes if SyncEveryWrite is false.
	SyncInterval time.Duration

	// MaxSegmentBytes is the maximum size of a single WAL segment before rolling.
	MaxSegmentBytes int64

	// DefaultLeaseTimeout is the duration a message is leased to a consumer before potential redelivery.
	DefaultLeaseTimeout time.Duration

	// MaxRedeliveries is the limit of lease timeouts before a message is flagged or sent to a DLQ.
	MaxRedeliveries int

	// LeaseCheckInterval is the frequency at which the broker checks for expired leases.
	LeaseCheckInterval time.Duration
}

// DefaultConfig returns a Config with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		DataDir:             "data",
		BindAddr:            ":8765",
		SyncEveryWrite:      true,
		SyncInterval:        200 * time.Millisecond,
		MaxSegmentBytes:     64 * 1024 * 1024, // 64 MB
		DefaultLeaseTimeout: 30 * time.Second,
		MaxRedeliveries:     5,
		LeaseCheckInterval:  200 * time.Millisecond,
	}
}
