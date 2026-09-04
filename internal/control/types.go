// Package control supervises independently running slipway configurations.
//
// It deliberately contains no transport or CLI concerns. Callers can expose a
// Manager over a Unix socket, HTTP, or another local control plane.
package control

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

// State is the lifecycle state of a managed slipway instance.
type State string

const (
	StateRunning  State = "running"
	StateStopping State = "stopping"
	StateExited   State = "exited"
	StateFailed   State = "failed"
)

var (
	// ErrNotFound means no instance matches a selector.
	ErrNotFound = errors.New("control: instance not found")
	// ErrAmbiguous means an ID prefix matches more than one instance.
	ErrAmbiguous = errors.New("control: ambiguous instance selector")
	// ErrAlreadyActive means a running or stopping instance already owns the
	// requested configuration path or queue database.
	ErrAlreadyActive = errors.New("control: instance already active")
	// ErrNameInUse means an instance (including a retained exited instance)
	// already owns an explicitly requested display name.
	ErrNameInUse = errors.New("control: instance name already in use")
	// ErrNotActive means a matched instance has already stopped.
	ErrNotActive = errors.New("control: instance is not active")
	// ErrShuttingDown means the manager no longer accepts new instances.
	ErrShuttingDown = errors.New("control: manager is shutting down")
	// ErrQueueChanged means a known queue's configuration now resolves to a
	// different database, so restarting it through the old queue identity would
	// silently target different durable state.
	ErrQueueChanged = errors.New("control: known queue configuration changed")
)

// Instance is an immutable point-in-time view of a managed runtime.
type Instance struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ConfigPath string `json:"config_path"`
	// ConfigHash identifies the effective configuration snapshot used by this
	// run. Unlike ID, it remains stable across runs when the configuration is
	// unchanged.
	ConfigHash   string     `json:"config_hash"`
	DatabasePath string     `json:"database_path"`
	State        State      `json:"state"`
	CreatedAt    time.Time  `json:"created_at"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	Error        string     `json:"error,omitempty"`
}

// RunOptions controls the lifecycle of an instance started through Client.RunWithOptions.
type RunOptions struct {
	// RemoveOnExit removes the terminal instance from the daemon's in-memory
	// registry. It does not remove or modify the durable queue.
	RemoveOnExit bool
}

// KnownQueue describes one durable queue successfully loaded during the
// daemon's lifetime. Unlike the bounded instance history, this catalog is kept
// until the daemon exits so queues remain discoverable after instances stop.
type KnownQueue struct {
	// Identity is the canonical database path used for stable internal
	// references. ConfigPath retains the lexical path used to load the config.
	Identity        string   `json:"-"`
	ConfigIdentity  string   `json:"-"`
	ConfigPath      string   `json:"config_path"`
	ConfigHash      string   `json:"config_hash"`
	DatabasePath    string   `json:"database_path"`
	DatabaseAliases []string `json:"-"`
	WatchNames      []string `json:"watch_names"`
}

// Active reports whether an instance still owns its configuration and queue
// database reservations.
func (instance Instance) Active() bool {
	return instance.State == StateRunning || instance.State == StateStopping
}

// Loader loads and validates one configuration file. The production default
// is config.Load.
type Loader func(string) (*config.Config, error)

// Runner runs one loaded configuration until its context is canceled or it
// encounters an error. The production default is daemon.Run.
type Runner func(context.Context, *config.Config, *slog.Logger) error

// IDGenerator returns a new 12-character hexadecimal instance ID.
type IDGenerator func() (string, error)

// Clock supplies lifecycle timestamps.
type Clock func() time.Time

// Options configures a Manager. Zero values select production defaults.
type Options struct {
	Context             context.Context
	Loader              Loader
	Runner              Runner
	IDGenerator         IDGenerator
	Clock               Clock
	Logger              *slog.Logger
	RetainedInstances   int
	LogCapacity         int
	LogSubscriberBuffer int
	MaxLogLineBytes     int
	MaxLogBytes         int
}
