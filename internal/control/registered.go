package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func (manager *Manager) startRegistered(ctx context.Context, paths []string, name string) ([]Instance, error) {
	if len(paths) == 0 {
		return nil, errors.New("control: at least one config path is required")
	}
	if name != "" && (len(paths) != 1 || !validDisplayName(name) || strings.TrimSpace(name) != name) {
		return nil, errors.New("control: a valid instance name may be supplied only for one config")
	}
	if err := manager.acquireStartGate(ctx); err != nil {
		return nil, err
	}
	defer manager.releaseStartGate()

	manager.mu.Lock()
	if manager.shuttingDown || manager.ctx.Err() != nil {
		manager.mu.Unlock()
		return nil, ErrShuttingDown
	}
	// An existing name/ID starts its stored snapshot without requiring YAML.
	if len(paths) == 1 && name == "" {
		runtime, err := manager.resolveLocked(paths[0])
		if err == nil {
			defer manager.mu.Unlock()
			return manager.resumeRegisteredLocked(ctx, runtime)
		}
		if errors.Is(err, ErrAmbiguous) {
			manager.mu.Unlock()
			return nil, err
		}
	}
	identities := make(map[string]string, len(manager.instances))
	for id, runtime := range manager.instances {
		identities[runtime.configIdentity] = id
	}
	manager.mu.Unlock()

	type loaded struct {
		path, identity, existingID string
		cfg                        *config.Config
	}
	items := make([]loaded, 0, len(paths))
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		absolute, err := absolutePath(path)
		if err != nil {
			return nil, err
		}
		identity, err := canonicalPath(absolute)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			equal, err := config.PathsEquivalent(identity, item.identity)
			if err != nil {
				return nil, err
			}
			if equal {
				return nil, duplicateConfigPathError(item.path, path, identity)
			}
		}
		var existingID string
		for existingIdentity, id := range identities {
			equal, err := config.PathsEquivalent(identity, existingIdentity)
			if err != nil {
				return nil, err
			}
			if equal {
				existingID = id
				identity = existingIdentity
				break
			}
		}
		cfg, err := manager.loader(absolute)
		if err != nil {
			return nil, fmt.Errorf("control: load config %s: %w", absolute, err)
		}
		if cfg == nil {
			return nil, fmt.Errorf("control: load config %s: loader returned nil", absolute)
		}
		// Copy the effective config through the same representation used on disk.
		// This strips already-expanded Values and avoids mutating a loader's cache.
		encoded, err := json.Marshal(cfg)
		if err != nil {
			return nil, err
		}
		var snapshot config.Config
		if err := json.Unmarshal(encoded, &snapshot); err != nil {
			return nil, err
		}
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		for wi := range snapshot.Watches {
			for ci := range snapshot.Watches[wi].Pipeline {
				command := &snapshot.Watches[wi].Pipeline[ci]
				if command.WorkingDir == "" {
					command.WorkingDir = cwd
				}
			}
		}
		items = append(items, loaded{absolute, identity, existingID, &snapshot})
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.shuttingDown || manager.ctx.Err() != nil {
		return nil, ErrShuttingDown
	}
	reservedIDs, reservedNames := map[string]struct{}{}, map[string]struct{}{}
	prepared := make([]registration, 0, len(items))
	for _, item := range items {
		view := Instance{ConfigPath: item.path, CreatedAt: manager.now()}
		if item.existingID != "" {
			previous := manager.instances[item.existingID]
			if previous.view.Active() {
				return nil, fmt.Errorf("%w: %s", ErrAlreadyActive, previous.view.Name)
			}
			view = cloneInstance(previous.view)
			view.ConfigPath = item.path
			if name != "" && name != view.Name {
				return nil, fmt.Errorf("%w: configuration is registered as %s", ErrNameInUse, view.Name)
			}
		} else {
			view.Name = name
			if view.Name == "" {
				view.Name = manager.availableNameLocked(automaticName(item.path), reservedNames, reservedIDs)
			} else if _, exists := manager.names[view.Name]; exists {
				return nil, fmt.Errorf("%w: %s", ErrNameInUse, view.Name)
			} else if _, exists := manager.instances[view.Name]; exists {
				return nil, fmt.Errorf("%w: name collides with instance ID", ErrNameInUse)
			}
			reservedNames[view.Name] = struct{}{}
			id, err := manager.availableIDLocked(reservedIDs, reservedNames)
			if err != nil {
				return nil, err
			}
			view.ID = id
			reservedIDs[id] = struct{}{}
			view.DatabasePath = filepath.Join(manager.registry.directory, "queues", id+".sqlite")
		}
		item.cfg.Database.Path = view.DatabasePath
		if err := item.cfg.Validate(); err != nil {
			return nil, fmt.Errorf("control: validate snapshot %s: %w", item.path, err)
		}
		hash, err := config.EffectiveFingerprint(item.cfg)
		if err != nil {
			return nil, err
		}
		view.ConfigHash = hash
		view.State, view.DesiredState = StateRunning, "running"
		view.StartedAt, view.FinishedAt, view.Error = manager.now(), nil, ""
		prepared = append(prepared, registration{view, item.identity, item.cfg})
	}
	if err := manager.registry.save(ctx, prepared); err != nil {
		return nil, err
	}
	result := make([]Instance, 0, len(prepared))
	for _, item := range prepared {
		manager.registerRuntime(item.view, item.identity, item.config, true)
		result = append(result, cloneInstance(item.view))
	}
	return result, nil
}

func (manager *Manager) resumeRegisteredLocked(ctx context.Context, runtime *runtimeInstance) ([]Instance, error) {
	if runtime.view.Active() {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyActive, runtime.view.Name)
	}
	if runtime.config == nil {
		return nil, fmt.Errorf("control: %s has an invalid snapshot; start it from a corrected config file", runtime.view.Name)
	}
	view := cloneInstance(runtime.view)
	view.State, view.DesiredState = StateRunning, "running"
	view.StartedAt, view.FinishedAt, view.Error = manager.now(), nil, ""
	if err := manager.registry.saveViews(ctx, []Instance{view}); err != nil {
		return nil, err
	}
	manager.registerRuntime(view, runtime.configIdentity, runtime.config, true)
	return []Instance{view}, nil
}

func (manager *Manager) startRegisteredQueue(ctx context.Context, known KnownQueue) ([]Instance, error) {
	if ctx == nil {
		return nil, errors.New("control: start context is required")
	}
	if err := manager.acquireStartGate(ctx); err != nil {
		return nil, err
	}
	defer manager.releaseStartGate()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.shuttingDown || manager.ctx.Err() != nil {
		return nil, ErrShuttingDown
	}
	for _, runtime := range manager.instances {
		if runtime.view.DatabasePath == known.Identity && runtime.configIdentity == known.ConfigIdentity {
			return manager.resumeRegisteredLocked(ctx, runtime)
		}
	}
	return nil, ErrNotFound
}

// Restore resumes desired instances from saved snapshots. Runner failures stay
// visible on individual instances and never prevent other instances starting.
// Call after acquiring listeners and before serving control requests.
func (manager *Manager) Restore(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, errors.New("control: restore context is required")
	}
	if manager.registry == nil {
		return 0, nil
	}
	if err := manager.acquireStartGate(ctx); err != nil {
		return 0, err
	}
	defer manager.releaseStartGate()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.shuttingDown || manager.ctx.Err() != nil {
		return 0, ErrShuttingDown
	}
	var pending []*runtimeInstance
	var views []Instance
	for _, runtime := range manager.instances {
		if runtime.view.DesiredState == "running" && !runtime.view.Active() && runtime.config != nil {
			view := cloneInstance(runtime.view)
			view.State, view.StartedAt, view.FinishedAt, view.Error = StateRunning, manager.now(), nil, ""
			pending = append(pending, runtime)
			views = append(views, view)
		}
	}
	if err := manager.registry.saveViews(ctx, views); err != nil {
		return 0, err
	}
	for index, runtime := range pending {
		manager.registerRuntime(views[index], runtime.configIdentity, runtime.config, true)
	}
	return len(pending), nil
}
