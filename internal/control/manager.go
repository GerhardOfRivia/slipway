package control

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/daemon"
)

const (
	maxIDGenerationAttempts  = 128
	defaultRetainedInstances = 100
	maxRetainedErrorBytes    = 16 * 1024
)

type runtimeInstance struct {
	view           Instance
	config         *config.Config
	configIdentity string
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	logs           *LogStream
	logger         *slog.Logger
}

// Attachment pins the runtime, completion signal, and log subscription for
// one attached start. It remains usable even after the manager's bounded
// terminal-instance registry evicts the instance.
type Attachment struct {
	manager      *Manager
	runtime      *runtimeInstance
	subscription *LogSubscription
}

// Lines returns retained logs followed by live logs until the instance exits
// or the attachment is canceled.
func (attachment *Attachment) Lines() <-chan string {
	if attachment == nil || attachment.subscription == nil {
		return nil
	}
	return attachment.subscription.Lines()
}

// Wait waits for the attached runtime to finish without resolving it through
// the manager's bounded registry.
func (attachment *Attachment) Wait(ctx context.Context) (Instance, error) {
	if attachment == nil || attachment.manager == nil || attachment.runtime == nil {
		return Instance{}, errors.New("control: attachment is required")
	}
	if ctx == nil {
		return Instance{}, errors.New("control: wait context is required")
	}
	return attachment.manager.waitForRuntime(ctx, attachment.runtime, attachment.runtime.done)
}

// Cancel releases the log subscription. It does not stop the runtime.
func (attachment *Attachment) Cancel() {
	if attachment != nil && attachment.subscription != nil {
		attachment.subscription.Cancel()
	}
}

// Manager owns all runtime goroutines for one control-plane daemon. Completed
// instances remain addressable for List(true), Wait, and retained logs.
type Manager struct {
	mu sync.Mutex
	// startGate serializes final filesystem identity checks and reservations
	// without holding mu, so a slow filesystem cannot block List, Stop, or
	// Shutdown. A channel makes waiting for the gate context-aware.
	startGate chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	loader      Loader
	runner      Runner
	idGenerator IDGenerator
	clock       Clock
	logger      *slog.Logger

	logCapacity         int
	logSubscriberBuffer int
	maxLogLineBytes     int
	maxLogBytes         int
	retainedInstances   int

	instances       map[string]*runtimeInstance
	names           map[string]string
	knownQueues     map[string]KnownQueue
	activeConfigs   map[string]string
	activeDatabases map[string]string
	shuttingDown    bool
}

// NewManager constructs an empty supervisor.
func NewManager(options Options) (*Manager, error) {
	if options.LogCapacity < 0 {
		return nil, errors.New("control: log capacity cannot be negative")
	}
	if options.LogSubscriberBuffer < 0 {
		return nil, errors.New("control: log subscriber buffer cannot be negative")
	}
	if options.MaxLogLineBytes < 0 {
		return nil, errors.New("control: maximum log line size cannot be negative")
	}
	if options.MaxLogBytes < 0 {
		return nil, errors.New("control: maximum retained log bytes cannot be negative")
	}
	if options.RetainedInstances < 0 {
		return nil, errors.New("control: retained instances cannot be negative")
	}
	parent := options.Context
	if parent == nil {
		parent = context.Background()
	}
	managerContext, cancel := context.WithCancel(parent)
	loader := options.Loader
	if loader == nil {
		loader = config.Load
	}
	runner := options.Runner
	if runner == nil {
		runner = daemon.Run
	}
	idGenerator := options.IDGenerator
	if idGenerator == nil {
		idGenerator = randomID
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	retainedInstances := options.RetainedInstances
	if retainedInstances == 0 {
		retainedInstances = defaultRetainedInstances
	}
	return &Manager{
		ctx:                 managerContext,
		cancel:              cancel,
		loader:              loader,
		runner:              runner,
		idGenerator:         idGenerator,
		clock:               clock,
		logger:              options.Logger,
		logCapacity:         options.LogCapacity,
		logSubscriberBuffer: options.LogSubscriberBuffer,
		maxLogLineBytes:     options.MaxLogLineBytes,
		maxLogBytes:         options.MaxLogBytes,
		retainedInstances:   retainedInstances,
		startGate:           make(chan struct{}, 1),
		instances:           make(map[string]*runtimeInstance),
		names:               make(map[string]string),
		knownQueues:         make(map[string]KnownQueue),
		activeConfigs:       make(map[string]string),
		activeDatabases:     make(map[string]string),
	}, nil
}

type startCandidate struct {
	configPath     string
	configIdentity string
	configHash     string
	databasePath   string
	queueIdentity  string
	config         *config.Config
	nameBase       string
}

type startResult struct {
	instances  []Instance
	attachment *Attachment
}

// StartMany loads and starts one instance per configuration path.
func (manager *Manager) StartMany(configPaths []string, name string) ([]Instance, error) {
	return manager.StartManyContext(context.Background(), configPaths, name)
}

// StartManyContext loads and starts one instance per configuration path. The
// operation is all-or-nothing with respect to validation and reservation: no
// runner is launched if any path, database, ID, requested name, or start
// context conflicts. The start context only bounds preflight; runtimes always
// inherit the manager context and therefore outlive the request that created
// them. name may be supplied only when starting exactly one configuration.
func (manager *Manager) StartManyContext(startContext context.Context, configPaths []string, name string) ([]Instance, error) {
	result, err := manager.startManyContext(startContext, configPaths, name, false, nil)
	return result.instances, err
}

// StartKnownQueueContext restarts the configuration associated with known,
// provided it still resolves to the same durable queue. This prevents a stale
// dashboard queue from silently starting a configuration that was edited to
// use another database.
func (manager *Manager) StartKnownQueueContext(startContext context.Context, known KnownQueue) ([]Instance, error) {
	if strings.TrimSpace(known.ConfigPath) == "" || strings.TrimSpace(known.ConfigIdentity) == "" || strings.TrimSpace(known.Identity) == "" {
		return nil, errors.New("control: known queue config path and identity are required")
	}
	copy := known
	copy.DatabaseAliases = append([]string(nil), known.DatabaseAliases...)
	result, err := manager.startManyContext(startContext, []string{known.ConfigPath}, "", false, &copy)
	return result.instances, err
}

// StartAttachedContext atomically starts exactly one instance and acquires its
// retained-plus-live log stream and completion signal before the runner can
// finish. This prevents retention eviction from racing attached clients.
func (manager *Manager) StartAttachedContext(startContext context.Context, configPath, name string) (Instance, *Attachment, error) {
	result, err := manager.startManyContext(startContext, []string{configPath}, name, true, nil)
	if err != nil {
		return Instance{}, nil, err
	}
	if len(result.instances) != 1 || result.attachment == nil {
		return Instance{}, nil, errors.New("control: attached start did not create exactly one instance")
	}
	return result.instances[0], result.attachment, nil
}

func (manager *Manager) startManyContext(startContext context.Context, configPaths []string, name string, attach bool, expectedQueue *KnownQueue) (startResult, error) {
	if manager == nil {
		return startResult{}, errors.New("control: manager is required")
	}
	if startContext == nil {
		return startResult{}, errors.New("control: start context is required")
	}
	if err := startContext.Err(); err != nil {
		return startResult{}, err
	}
	manager.mu.Lock()
	shuttingDown := manager.shuttingDown || manager.ctx.Err() != nil
	manager.mu.Unlock()
	if shuttingDown {
		return startResult{}, ErrShuttingDown
	}
	if len(configPaths) == 0 {
		return startResult{}, errors.New("control: at least one config path is required")
	}
	if attach && len(configPaths) != 1 {
		return startResult{}, errors.New("control: attached start requires exactly one config")
	}
	explicitName := strings.TrimSpace(name)
	if name != "" {
		if len(configPaths) != 1 {
			return startResult{}, errors.New("control: a name may be supplied only for one config")
		}
		if explicitName != name || !validDisplayName(explicitName) {
			return startResult{}, fmt.Errorf("control: invalid instance name %q", name)
		}
	}

	candidates := make([]startCandidate, 0, len(configPaths))
	type configOwner struct {
		requested string
		identity  string
		info      os.FileInfo
	}
	configOwners := make(map[string]configOwner, len(configPaths))
	configOwnersByFold := make(map[string][]configOwner, len(configPaths))
	existingConfigs := make([]configOwner, 0, len(configPaths))
	for _, requestedPath := range configPaths {
		if err := startContext.Err(); err != nil {
			return startResult{}, err
		}
		configPath, err := absolutePath(requestedPath)
		if err != nil {
			return startResult{}, fmt.Errorf("control: resolve config path %q: %w", requestedPath, err)
		}
		configIdentity, err := canonicalPath(configPath)
		if err != nil {
			return startResult{}, fmt.Errorf("control: canonicalize config path %q: %w", requestedPath, err)
		}
		if owner, exists := configOwners[configIdentity]; exists {
			return startResult{}, duplicateConfigPathError(owner.requested, requestedPath, configIdentity)
		}
		configInfo, err := statDatabaseFile(configIdentity)
		if err != nil {
			return startResult{}, fmt.Errorf("control: inspect config path %q: %w", requestedPath, err)
		}
		if configInfo != nil {
			for _, owner := range existingConfigs {
				if os.SameFile(owner.info, configInfo) {
					return startResult{}, duplicateConfigPathError(owner.requested, requestedPath, configIdentity)
				}
			}
		}
		foldedIdentity := strings.ToLower(configIdentity)
		for _, owner := range configOwnersByFold[foldedIdentity] {
			equivalent, compareErr := config.PathsEquivalent(owner.identity, configIdentity)
			if compareErr != nil {
				return startResult{}, fmt.Errorf("control: compare config paths %q and %q: %w", owner.requested, requestedPath, compareErr)
			}
			if equivalent {
				return startResult{}, duplicateConfigPathError(owner.requested, requestedPath, configIdentity)
			}
		}
		cfg, err := manager.loader(configPath)
		if err != nil {
			return startResult{}, fmt.Errorf("control: load config %s: %w", configPath, err)
		}
		if err := startContext.Err(); err != nil {
			return startResult{}, err
		}
		if cfg == nil {
			return startResult{}, fmt.Errorf("control: load config %s: loader returned nil", configPath)
		}
		configHash, err := config.EffectiveFingerprint(cfg)
		if err != nil {
			return startResult{}, fmt.Errorf("control: fingerprint config %s: %w", configPath, err)
		}
		databasePath, err := config.CanonicalDatabasePath(cfg.Database.Path)
		if err != nil {
			return startResult{}, fmt.Errorf("control: resolve database for %s: %w", configPath, err)
		}
		owner := configOwner{requested: requestedPath, identity: configIdentity, info: configInfo}
		configOwners[configIdentity] = owner
		configOwnersByFold[foldedIdentity] = append(configOwnersByFold[foldedIdentity], owner)
		if configInfo != nil {
			existingConfigs = append(existingConfigs, owner)
		}
		candidates = append(candidates, startCandidate{
			configPath:     configPath,
			configIdentity: configIdentity,
			configHash:     configHash,
			databasePath:   databasePath,
			queueIdentity:  databasePath,
			config:         cfg,
			nameBase:       automaticName(configPath),
		})
	}
	if err := validateDatabaseCandidates(startContext, candidates); err != nil {
		return startResult{}, err
	}
	if err := manager.acquireStartGate(startContext); err != nil {
		return startResult{}, err
	}
	defer manager.releaseStartGate()

	// Recheck filesystem identity after acquiring the start gate. This catches
	// aliases introduced during config loading while keeping filesystem I/O off
	// the registry mutex.
	if err := validateDatabaseCandidates(startContext, candidates); err != nil {
		return startResult{}, err
	}
	if err := validateExpectedQueue(startContext, candidates, expectedQueue); err != nil {
		return startResult{}, err
	}
	if err := assignKnownQueueIdentities(startContext, candidates, manager.KnownQueues()); err != nil {
		return startResult{}, err
	}

	// Active runtimes may finish while their filesystem identities are being
	// inspected. Validate a snapshot, then reserve only if that snapshot still
	// matches the maps under mu. Other starts cannot add reservations while this
	// goroutine owns startGate; removals simply cause a retry.
	for {
		if err := startContext.Err(); err != nil {
			return startResult{}, err
		}
		manager.mu.Lock()
		if manager.shuttingDown || manager.ctx.Err() != nil {
			manager.mu.Unlock()
			return startResult{}, ErrShuttingDown
		}
		activeConfigs := maps.Clone(manager.activeConfigs)
		activeDatabases := maps.Clone(manager.activeDatabases)
		manager.mu.Unlock()

		reservationErr := validateActiveCandidates(startContext, candidates, activeConfigs, activeDatabases)

		manager.mu.Lock()
		if err := startContext.Err(); err != nil {
			manager.mu.Unlock()
			return startResult{}, err
		}
		if manager.shuttingDown || manager.ctx.Err() != nil {
			manager.mu.Unlock()
			return startResult{}, ErrShuttingDown
		}
		if !maps.Equal(activeConfigs, manager.activeConfigs) || !maps.Equal(activeDatabases, manager.activeDatabases) {
			manager.mu.Unlock()
			continue
		}
		if reservationErr != nil {
			manager.mu.Unlock()
			return startResult{}, reservationErr
		}
		break
	}
	defer manager.mu.Unlock()

	if explicitName != "" {
		if owner, exists := manager.names[explicitName]; exists {
			return startResult{}, fmt.Errorf("%w: %q is owned by %s", ErrNameInUse, explicitName, owner)
		}
		if _, exists := manager.instances[explicitName]; exists {
			return startResult{}, fmt.Errorf("%w: %q collides with an instance ID", ErrNameInUse, explicitName)
		}
	}

	reservedNames := make(map[string]struct{}, len(candidates))
	reservedIDs := make(map[string]struct{}, len(candidates))
	type preparedInstance struct {
		candidate startCandidate
		id        string
		name      string
	}
	prepared := make([]preparedInstance, 0, len(candidates))
	for index, candidate := range candidates {
		displayName := explicitName
		if displayName == "" {
			displayName = manager.availableNameLocked(candidate.nameBase, reservedNames, reservedIDs)
		}
		reservedNames[displayName] = struct{}{}
		id, err := manager.availableIDLocked(reservedIDs, reservedNames)
		if err != nil {
			return startResult{}, err
		}
		reservedIDs[id] = struct{}{}
		prepared = append(prepared, preparedInstance{candidate: candidate, id: id, name: displayName})
		// Only one item can use an explicit name, but clearing it makes the
		// invariant obvious if this method is changed later.
		if index == 0 {
			explicitName = ""
		}
	}
	if err := startContext.Err(); err != nil {
		return startResult{}, err
	}

	result := startResult{instances: make([]Instance, 0, len(prepared))}
	for _, item := range prepared {
		now := manager.now()
		runContext, cancel := context.WithCancel(manager.ctx)
		logs := newLogStream(manager.logCapacity, manager.logSubscriberBuffer, manager.maxLogLineBytes, manager.maxLogBytes)
		view := Instance{
			ID:           item.id,
			Name:         item.name,
			ConfigPath:   item.candidate.configPath,
			ConfigHash:   item.candidate.configHash,
			DatabasePath: item.candidate.databasePath,
			State:        StateRunning,
			CreatedAt:    now,
			StartedAt:    now,
		}
		runtime := &runtimeInstance{
			view:           view,
			config:         item.candidate.config,
			configIdentity: item.candidate.configIdentity,
			ctx:            runContext,
			cancel:         cancel,
			done:           make(chan struct{}),
			logs:           logs,
		}
		runtime.logger = manager.instanceLogger(runtime)
		manager.instances[view.ID] = runtime
		manager.names[view.Name] = view.ID
		watchNames := make([]string, 0, len(item.candidate.config.Watches))
		for _, watch := range item.candidate.config.Watches {
			watchNames = append(watchNames, watch.Name)
		}
		known, exists := manager.knownQueues[item.candidate.queueIdentity]
		if !exists {
			known.Identity = item.candidate.queueIdentity
			known.DatabasePath = item.candidate.queueIdentity
		}
		known.ConfigIdentity = item.candidate.configIdentity
		known.ConfigPath = item.candidate.configPath
		known.ConfigHash = item.candidate.configHash
		known.WatchNames = watchNames
		if item.candidate.databasePath != known.DatabasePath && !slices.Contains(known.DatabaseAliases, item.candidate.databasePath) {
			known.DatabaseAliases = append(known.DatabaseAliases, item.candidate.databasePath)
		}
		manager.knownQueues[item.candidate.queueIdentity] = known
		manager.activeConfigs[runtime.configIdentity] = view.ID
		manager.activeDatabases[view.DatabasePath] = view.ID
		result.instances = append(result.instances, cloneInstance(view))
		if attach {
			result.attachment = &Attachment{
				manager:      manager,
				runtime:      runtime,
				subscription: logs.Subscribe(),
			}
		}
		// Launch while holding the manager mutex so Shutdown cannot observe a
		// reserved instance whose goroutine and done channel are not live yet.
		go manager.run(runtime)
	}
	return result, nil
}

// KnownQueues returns every durable queue successfully loaded during this
// daemon lifetime. The catalog is independent of bounded instance retention so
// stopped queues stay available to local management surfaces such as the web
// dashboard.
func (manager *Manager) KnownQueues() []KnownQueue {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	queues := make([]KnownQueue, 0, len(manager.knownQueues))
	for _, known := range manager.knownQueues {
		copy := known
		copy.DatabaseAliases = append([]string(nil), known.DatabaseAliases...)
		copy.WatchNames = append([]string(nil), known.WatchNames...)
		queues = append(queues, copy)
	}
	manager.mu.Unlock()
	sort.Slice(queues, func(left, right int) bool {
		if queues[left].ConfigPath != queues[right].ConfigPath {
			return queues[left].ConfigPath < queues[right].ConfigPath
		}
		return queues[left].Identity < queues[right].Identity
	})
	return queues
}

func validateExpectedQueue(ctx context.Context, candidates []startCandidate, expected *KnownQueue) error {
	if expected == nil {
		return nil
	}
	if len(candidates) != 1 {
		return errors.New("control: a known queue start requires exactly one configuration")
	}
	candidate := candidates[0]
	if candidate.configIdentity != expected.ConfigIdentity {
		return fmt.Errorf("%w: config %q no longer resolves to its registered file", ErrQueueChanged, candidate.configPath)
	}
	paths := make([]string, 0, 2+len(expected.DatabaseAliases))
	paths = append(paths, expected.Identity, expected.DatabasePath)
	paths = append(paths, expected.DatabaseAliases...)
	for _, expectedPath := range paths {
		if strings.TrimSpace(expectedPath) == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		equivalent, err := config.PathsEquivalent(candidate.databasePath, expectedPath)
		if err != nil {
			return fmt.Errorf("control: compare known queue database %q with %q: %w", candidate.databasePath, expectedPath, err)
		}
		if equivalent {
			return nil
		}
	}
	return fmt.Errorf("%w: config %q now uses database %q instead of registered queue %q", ErrQueueChanged, candidate.configPath, candidate.databasePath, expected.Identity)
}

func assignKnownQueueIdentities(ctx context.Context, candidates []startCandidate, knownQueues []KnownQueue) error {
	for index := range candidates {
		candidate := &candidates[index]
		candidate.queueIdentity = candidate.databasePath
	findKnown:
		for _, known := range knownQueues {
			paths := make([]string, 0, 2+len(known.DatabaseAliases))
			paths = append(paths, known.Identity, known.DatabasePath)
			paths = append(paths, known.DatabaseAliases...)
			for _, knownPath := range paths {
				if strings.TrimSpace(knownPath) == "" {
					continue
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				equivalent, err := config.PathsEquivalent(candidate.databasePath, knownPath)
				if err != nil {
					return fmt.Errorf("control: compare queue database %q with known database %q: %w", candidate.databasePath, knownPath, err)
				}
				if equivalent {
					candidate.queueIdentity = known.Identity
					break findKnown
				}
			}
		}
	}
	return nil
}

func (manager *Manager) acquireStartGate(ctx context.Context) error {
	select {
	case manager.startGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-manager.ctx.Done():
		return ErrShuttingDown
	}
}

func (manager *Manager) releaseStartGate() {
	<-manager.startGate
}

func duplicateConfigPathError(owner, requested, identity string) error {
	return fmt.Errorf("control: config paths %q and %q resolve to the same path %q", owner, requested, identity)
}

func validateDatabaseCandidates(ctx context.Context, candidates []startCandidate) error {
	type existingDatabase struct {
		candidate startCandidate
		info      os.FileInfo
	}
	paths := make(map[string]startCandidate, len(candidates))
	existing := make([]existingDatabase, 0, len(candidates))
	missing := make([]startCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if owner, found := paths[candidate.databasePath]; found {
			return duplicateDatabaseError(owner, candidate)
		}
		paths[candidate.databasePath] = candidate
		info, err := statDatabaseFile(candidate.databasePath)
		if err != nil {
			return fmt.Errorf("control: inspect database for config %q: %w", candidate.configPath, err)
		}
		if info == nil {
			for _, owner := range missing {
				equivalent, err := config.PathsEquivalent(owner.databasePath, candidate.databasePath)
				if err != nil {
					return fmt.Errorf("control: compare databases for configs %q and %q: %w", owner.configPath, candidate.configPath, err)
				}
				if equivalent {
					return duplicateDatabaseError(owner, candidate)
				}
			}
			missing = append(missing, candidate)
			continue
		}
		for _, owner := range existing {
			if os.SameFile(owner.info, info) {
				return duplicateDatabaseError(owner.candidate, candidate)
			}
		}
		existing = append(existing, existingDatabase{candidate: candidate, info: info})
	}
	return nil
}

func validateActiveCandidates(
	ctx context.Context,
	candidates []startCandidate,
	activeConfigs map[string]string,
	activeDatabases map[string]string,
) error {
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		owner, exists, err := activeConfigOwner(ctx, candidate.configIdentity, activeConfigs)
		if err != nil {
			return fmt.Errorf("control: compare config %q with active instances: %w", candidate.configPath, err)
		}
		if exists {
			return fmt.Errorf("%w: config %q is owned by %s", ErrAlreadyActive, candidate.configPath, owner)
		}
		owner, exists, err = activeDatabaseOwner(ctx, candidate.databasePath, activeDatabases)
		if err != nil {
			return fmt.Errorf("control: compare database %q with active instances: %w", candidate.databasePath, err)
		}
		if exists {
			return fmt.Errorf("%w: database %q is owned by %s", ErrAlreadyActive, candidate.databasePath, owner)
		}
	}
	return nil
}

func activeConfigOwner(ctx context.Context, configPath string, activeConfigs map[string]string) (string, bool, error) {
	if owner, exists := activeConfigs[configPath]; exists {
		return owner, true, nil
	}
	candidateInfo, err := statDatabaseFile(configPath)
	if err != nil {
		return "", false, err
	}
	for activePath, owner := range activeConfigs {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		if candidateInfo != nil {
			activeInfo, err := statDatabaseFile(activePath)
			if err != nil {
				return "", false, err
			}
			if activeInfo != nil && os.SameFile(candidateInfo, activeInfo) {
				return owner, true, nil
			}
		}
		if strings.EqualFold(configPath, activePath) {
			equivalent, err := config.PathsEquivalent(configPath, activePath)
			if err != nil {
				return "", false, err
			}
			if equivalent {
				return owner, true, nil
			}
		}
	}
	return "", false, nil
}

func duplicateDatabaseError(owner, candidate startCandidate) error {
	return fmt.Errorf(
		"control: configs %q and %q use the same database %q",
		owner.configPath,
		candidate.configPath,
		candidate.databasePath,
	)
}

func activeDatabaseOwner(ctx context.Context, databasePath string, activeDatabases map[string]string) (string, bool, error) {
	if owner, exists := activeDatabases[databasePath]; exists {
		return owner, true, nil
	}
	candidateInfo, err := statDatabaseFile(databasePath)
	if err != nil {
		return "", false, err
	}
	for activePath, owner := range activeDatabases {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		activeInfo, err := statDatabaseFile(activePath)
		if err != nil {
			return "", false, err
		}
		if candidateInfo != nil && activeInfo != nil && os.SameFile(candidateInfo, activeInfo) {
			return owner, true, nil
		}
		equivalent, err := config.PathsEquivalent(databasePath, activePath)
		if err != nil {
			return "", false, err
		}
		if equivalent {
			return owner, true, nil
		}
	}
	return "", false, nil
}

func statDatabaseFile(path string) (os.FileInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	return info, nil
}

// List returns running and stopping instances, or all retained instances when
// all is true. Results are ordered newest-first, then by name and ID.
func (manager *Manager) List(all bool) []Instance {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	instances := make([]Instance, 0, len(manager.instances))
	for _, runtime := range manager.instances {
		if all || runtime.view.Active() {
			instances = append(instances, cloneInstance(runtime.view))
		}
	}
	manager.mu.Unlock()
	sort.Slice(instances, func(left, right int) bool {
		if !instances[left].CreatedAt.Equal(instances[right].CreatedAt) {
			return instances[left].CreatedAt.After(instances[right].CreatedAt)
		}
		if instances[left].Name != instances[right].Name {
			return instances[left].Name < instances[right].Name
		}
		return instances[left].ID < instances[right].ID
	})
	return instances
}

// Stop cancels one active instance selected by exact name or an unambiguous ID
// prefix, then waits for its runner and log stream to finish.
func (manager *Manager) Stop(ctx context.Context, selector string) (Instance, error) {
	if manager == nil {
		return Instance{}, errors.New("control: manager is required")
	}
	if ctx == nil {
		return Instance{}, errors.New("control: stop context is required")
	}
	if err := ctx.Err(); err != nil {
		return Instance{}, err
	}
	manager.mu.Lock()
	runtime, err := manager.resolveLocked(selector)
	if err != nil {
		manager.mu.Unlock()
		return Instance{}, err
	}
	if !runtime.view.Active() {
		view := cloneInstance(runtime.view)
		manager.mu.Unlock()
		return view, fmt.Errorf("%w: %s is %s", ErrNotActive, runtime.view.Name, runtime.view.State)
	}
	if runtime.view.State == StateRunning {
		runtime.view.State = StateStopping
	}
	cancel := runtime.cancel
	done := runtime.done
	manager.mu.Unlock()

	cancel()
	return manager.waitForRuntime(ctx, runtime, done)
}

// Wait waits for a selected instance to reach a terminal state. It works for
// both active and already-completed retained instances.
func (manager *Manager) Wait(ctx context.Context, selector string) (Instance, error) {
	if manager == nil {
		return Instance{}, errors.New("control: manager is required")
	}
	if ctx == nil {
		return Instance{}, errors.New("control: wait context is required")
	}
	manager.mu.Lock()
	runtime, err := manager.resolveLocked(selector)
	if err != nil {
		manager.mu.Unlock()
		return Instance{}, err
	}
	done := runtime.done
	manager.mu.Unlock()
	return manager.waitForRuntime(ctx, runtime, done)
}

// Logs subscribes to retained and live logs for a selected instance.
func (manager *Manager) Logs(selector string) (*LogSubscription, error) {
	if manager == nil {
		return nil, errors.New("control: manager is required")
	}
	manager.mu.Lock()
	runtime, err := manager.resolveLocked(selector)
	if err != nil {
		manager.mu.Unlock()
		return nil, err
	}
	logs := runtime.logs
	manager.mu.Unlock()
	return logs.Subscribe(), nil
}

// Shutdown rejects future starts, cancels every active instance, and waits for
// all retained runtime goroutines to finish or ctx to expire. It is idempotent.
func (manager *Manager) Shutdown(ctx context.Context) error {
	if manager == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("control: shutdown context is required")
	}
	manager.mu.Lock()
	manager.shuttingDown = true
	runtimes := make([]*runtimeInstance, 0, len(manager.instances))
	for _, runtime := range manager.instances {
		if runtime.view.Active() {
			if runtime.view.State == StateRunning {
				runtime.view.State = StateStopping
			}
			runtimes = append(runtimes, runtime)
		}
	}
	manager.mu.Unlock()

	manager.cancel()
	for _, runtime := range runtimes {
		runtime.cancel()
	}
	for _, runtime := range runtimes {
		select {
		case <-runtime.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (manager *Manager) run(runtime *runtimeInstance) {
	runtime.logger.Info("instance running")
	err := manager.invokeRunner(runtime)

	manager.mu.Lock()
	contextErr := runtime.ctx.Err()
	stopping := runtime.view.State == StateStopping || contextErr != nil
	view := cloneInstance(runtime.view)
	manager.mu.Unlock()

	if err != nil && !(stopping && cancellationFrom(err, contextErr)) {
		view.State = StateFailed
		view.Error = boundedError(err.Error())
	} else {
		view.State = StateExited
		view.Error = ""
	}
	finishedAt := manager.now()
	view.FinishedAt = &finishedAt

	// Keep the instance active until all final logging is complete and the log
	// stream is closed. Shutdown therefore cannot miss a goroutine that is
	// still finalizing merely because its terminal state was published early.
	if view.State == StateFailed {
		runtime.logger.Error("instance failed", "error", view.Error)
	} else {
		runtime.logger.Info("instance exited")
	}
	_ = runtime.logs.Close()

	manager.mu.Lock()
	runtime.view = view
	if manager.activeConfigs[runtime.configIdentity] == runtime.view.ID {
		delete(manager.activeConfigs, runtime.configIdentity)
	}
	if manager.activeDatabases[runtime.view.DatabasePath] == runtime.view.ID {
		delete(manager.activeDatabases, runtime.view.DatabasePath)
	}
	// Publish terminal state and completion together under the manager mutex.
	// Waiters that observe done closed cannot race an incomplete final view.
	close(runtime.done)
	manager.trimTerminatedLocked(runtime.view.ID)
	manager.mu.Unlock()
}

func (manager *Manager) invokeRunner(runtime *runtimeInstance) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("runner panic: %v", recovered)
			runtime.logger.Error("instance runner panicked", "panic", recovered, "stack", string(debug.Stack()))
		}
	}()
	return manager.runner(runtime.ctx, runtime.config, runtime.logger)
}

func (manager *Manager) waitForRuntime(ctx context.Context, runtime *runtimeInstance, done <-chan struct{}) (Instance, error) {
	select {
	case <-done:
		manager.mu.Lock()
		view := cloneInstance(runtime.view)
		manager.mu.Unlock()
		return view, nil
	default:
	}
	select {
	case <-done:
		manager.mu.Lock()
		view := cloneInstance(runtime.view)
		manager.mu.Unlock()
		return view, nil
	case <-ctx.Done():
		return Instance{}, ctx.Err()
	}
}

func (manager *Manager) resolveLocked(selector string) (*runtimeInstance, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil, fmt.Errorf("%w: empty selector", ErrNotFound)
	}
	if id, exists := manager.names[selector]; exists {
		return manager.instances[id], nil
	}
	var matched *runtimeInstance
	for id, runtime := range manager.instances {
		if !strings.HasPrefix(id, selector) {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("%w: ID prefix %q", ErrAmbiguous, selector)
		}
		matched = runtime
	}
	if matched == nil {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, selector)
	}
	return matched, nil
}

func (manager *Manager) availableIDLocked(reservedIDs, reservedNames map[string]struct{}) (string, error) {
	for attempt := 0; attempt < maxIDGenerationAttempts; attempt++ {
		id, err := manager.idGenerator()
		if err != nil {
			return "", fmt.Errorf("control: generate instance ID: %w", err)
		}
		id = strings.ToLower(strings.TrimSpace(id))
		if !validID(id) {
			return "", fmt.Errorf("control: ID generator returned invalid 12-hex ID %q", id)
		}
		if _, exists := manager.instances[id]; exists {
			continue
		}
		if _, exists := manager.names[id]; exists {
			continue
		}
		if _, exists := reservedIDs[id]; exists {
			continue
		}
		if _, exists := reservedNames[id]; exists {
			continue
		}
		return id, nil
	}
	return "", errors.New("control: could not generate a unique instance ID")
}

func (manager *Manager) availableNameLocked(base string, reservedNames, reservedIDs map[string]struct{}) string {
	for suffix := 1; ; suffix++ {
		candidate := base
		if suffix > 1 {
			candidate = fmt.Sprintf("%s-%d", base, suffix)
		}
		if _, exists := manager.names[candidate]; exists {
			continue
		}
		if _, exists := manager.instances[candidate]; exists {
			continue
		}
		if _, exists := reservedNames[candidate]; exists {
			continue
		}
		if _, exists := reservedIDs[candidate]; exists {
			continue
		}
		return candidate
	}
}

func (manager *Manager) instanceLogger(runtime *runtimeInstance) *slog.Logger {
	streamHandler := slog.NewTextHandler(runtime.logs, &slog.HandlerOptions{Level: slog.LevelDebug})
	handlers := []slog.Handler{streamHandler}
	if manager.logger != nil {
		handlers = append(handlers, manager.logger.Handler())
	}
	return slog.New(fanoutHandler{handlers: handlers}).With(
		"instance_id", runtime.view.ID,
		"instance", runtime.view.Name,
		"config", runtime.view.ConfigPath,
		"config_hash", runtime.view.ConfigHash,
	)
}

func (manager *Manager) now() time.Time {
	return manager.clock().UTC()
}

func (manager *Manager) trimTerminatedLocked(currentID string) {
	terminalIDs := make([]string, 0, len(manager.instances))
	terminalCount := 0
	for id, runtime := range manager.instances {
		if runtime.view.Active() {
			continue
		}
		terminalCount++
		if id != currentID {
			terminalIDs = append(terminalIDs, id)
		}
	}
	excess := terminalCount - manager.retainedInstances
	if excess <= 0 {
		return
	}
	sort.Slice(terminalIDs, func(left, right int) bool {
		leftView := manager.instances[terminalIDs[left]].view
		rightView := manager.instances[terminalIDs[right]].view
		if leftView.FinishedAt != nil && rightView.FinishedAt != nil && !leftView.FinishedAt.Equal(*rightView.FinishedAt) {
			return leftView.FinishedAt.Before(*rightView.FinishedAt)
		}
		if !leftView.CreatedAt.Equal(rightView.CreatedAt) {
			return leftView.CreatedAt.Before(rightView.CreatedAt)
		}
		return leftView.ID < rightView.ID
	})
	if excess > len(terminalIDs) {
		excess = len(terminalIDs)
	}
	for _, id := range terminalIDs[:excess] {
		runtime := manager.instances[id]
		delete(manager.instances, id)
		if manager.names[runtime.view.Name] == id {
			delete(manager.names, runtime.view.Name)
		}
	}
}

func boundedError(value string) string {
	if len(value) <= maxRetainedErrorBytes {
		return value
	}
	const suffix = "... [truncated]"
	return value[:maxRetainedErrorBytes-len(suffix)] + suffix
}

func canonicalPath(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", errors.New("path is empty")
	}
	return config.CanonicalDatabasePath(value)
}

func absolutePath(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", errors.New("path is empty")
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}

func randomID() (string, error) {
	var value [6]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func validID(id string) bool {
	if len(id) != 12 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func automaticName(configPath string) string {
	base := filepath.Base(configPath)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	var normalized strings.Builder
	previousSeparator := false
	for _, character := range base {
		allowed := unicode.IsLetter(character) || unicode.IsDigit(character) || character == '_' || character == '.' || character == '-'
		if allowed {
			normalized.WriteRune(character)
			previousSeparator = false
			continue
		}
		if !previousSeparator {
			normalized.WriteByte('-')
			previousSeparator = true
		}
	}
	name := strings.Trim(normalized.String(), ".-")
	if name == "" {
		return "slipway"
	}
	return name
}

func validDisplayName(name string) bool {
	for index, character := range name {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			continue
		}
		if index > 0 && (character == '_' || character == '.' || character == '-') {
			continue
		}
		return false
	}
	return name != ""
}

func cloneInstance(instance Instance) Instance {
	cloned := instance
	if instance.FinishedAt != nil {
		finishedAt := *instance.FinishedAt
		cloned.FinishedAt = &finishedAt
	}
	return cloned
}

// cancellationFrom reports whether err consists solely of wrappers around the
// supplied context error. A joined operational error is not hidden merely
// because it also contains cancellation.
func cancellationFrom(err, contextErr error) bool {
	if err == nil || contextErr == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !cancellationFrom(cause, contextErr) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return cancellationFrom(wrapped.Unwrap(), contextErr)
	}
	return errors.Is(err, contextErr)
}
