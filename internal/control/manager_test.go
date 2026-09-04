package control

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func TestManagerStartStopListAndRetain(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	alphaPath := filepath.Join(root, "alpha.yaml")
	betaPath := filepath.Join(root, "beta.yml")
	alphaConfig := testConfig(filepath.Join(root, "alpha.db"))
	betaConfig := testConfig(filepath.Join(root, "beta.db"))
	startedRuns := make(chan *config.Config, 2)

	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			alphaPath: alphaConfig,
			betaPath:  betaConfig,
		}),
		Runner: func(ctx context.Context, cfg *config.Config, _ *slog.Logger) error {
			startedRuns <- cfg
			<-ctx.Done()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002"),
		Clock:       sequenceClock(time.Date(2026, 8, 25, 12, 0, 0, 0, time.FixedZone("test", -6*60*60))),
	})

	started, err := manager.StartMany([]string{alphaPath, betaPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(started), 2; got != want {
		t.Fatalf("started instances = %d, want %d", got, want)
	}
	if started[0].ID != "000000000001" || started[0].Name != "alpha" || started[0].State != StateRunning {
		t.Fatalf("first instance = %+v", started[0])
	}
	if started[1].ID != "000000000002" || started[1].Name != "beta" || started[1].State != StateRunning {
		t.Fatalf("second instance = %+v", started[1])
	}
	if started[0].CreatedAt.Location() != time.UTC || started[1].CreatedAt.Location() != time.UTC {
		t.Fatalf("timestamps were not normalized to UTC: %v, %v", started[0].CreatedAt, started[1].CreatedAt)
	}
	waitForRunnerStarts(t, startedRuns, 2)

	active := manager.List(false)
	if got := instanceNames(active); !reflect.DeepEqual(got, []string{"beta", "alpha"}) {
		t.Fatalf("active order = %v, want newest first", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopped, err := manager.Stop(ctx, "00000000000")
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("ambiguous stop error = %v, want ErrAmbiguous", err)
	}
	stopped, err = manager.Stop(ctx, "000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != StateExited || stopped.FinishedAt == nil || stopped.Error != "" {
		t.Fatalf("stopped instance = %+v", stopped)
	}
	if got := instanceNames(manager.List(false)); !reflect.DeepEqual(got, []string{"beta"}) {
		t.Fatalf("active after stop = %v", got)
	}
	all := manager.List(true)
	if got := instanceNames(all); !reflect.DeepEqual(got, []string{"beta", "alpha"}) {
		t.Fatalf("retained instances = %v", got)
	}

	stopped, err = manager.Stop(ctx, "beta")
	if err != nil || stopped.State != StateExited {
		t.Fatalf("stop by name = %+v, %v", stopped, err)
	}
	if got := manager.List(false); len(got) != 0 {
		t.Fatalf("active instances after both stops = %+v", got)
	}
}

func TestManagerRetainsKnownQueuesAfterInstancesStop(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	configPath := filepath.Join(root, "incoming.yaml")
	databasePath := filepath.Join(root, "incoming.db")
	cfg := testConfig(databasePath)
	cfg.Watches = []config.WatchConfig{{Name: "incoming"}, {Name: "archive"}}
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{configPath: cfg}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			<-ctx.Done()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})

	instances, err := manager.StartMany([]string{configPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	known := manager.KnownQueues()
	if len(known) != 1 {
		t.Fatalf("known queues = %+v, want one", known)
	}
	if known[0].ConfigPath != configPath || known[0].DatabasePath != databasePath ||
		!reflect.DeepEqual(known[0].WatchNames, []string{"incoming", "archive"}) {
		t.Fatalf("known queue = %+v", known[0])
	}
	known[0].WatchNames[0] = "mutated"
	if got := manager.KnownQueues()[0].WatchNames[0]; got != "incoming" {
		t.Fatalf("known queue returned mutable watch slice: %q", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := manager.Stop(ctx, instances[0].ID); err != nil {
		t.Fatal(err)
	}
	if len(manager.List(false)) != 0 || len(manager.KnownQueues()) != 1 {
		t.Fatalf("stopped queue was not retained: active=%+v queues=%+v", manager.List(false), manager.KnownQueues())
	}
}

func TestManagerCoalescesSequentialDatabaseAliases(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	primaryDatabase := filepath.Join(root, "queue.db")
	aliasDatabase := filepath.Join(root, "queue-alias.db")
	if err := os.WriteFile(primaryDatabase, []byte("queue identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(primaryDatabase, aliasDatabase); err != nil {
		t.Fatal(err)
	}
	firstConfig := filepath.Join(root, "first.yaml")
	secondConfig := filepath.Join(root, "second.yaml")
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			firstConfig:  testConfig(primaryDatabase),
			secondConfig: testConfig(aliasDatabase),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			<-ctx.Done()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002", "000000000003"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first, err := manager.StartMany([]string{firstConfig}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Stop(ctx, first[0].ID); err != nil {
		t.Fatal(err)
	}
	second, err := manager.StartMany([]string{secondConfig}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Stop(ctx, second[0].ID); err != nil {
		t.Fatal(err)
	}

	known := manager.KnownQueues()
	if len(known) != 1 {
		t.Fatalf("known queues = %+v, want one physical queue", known)
	}
	if known[0].Identity != primaryDatabase || known[0].DatabasePath != primaryDatabase ||
		known[0].ConfigPath != secondConfig || !slices.Contains(known[0].DatabaseAliases, aliasDatabase) {
		t.Fatalf("coalesced known queue = %+v", known[0])
	}
	restarted, err := manager.StartKnownQueueContext(ctx, known[0])
	if err != nil {
		t.Fatalf("restart through coalesced alias: %v", err)
	}
	if _, err := manager.Stop(ctx, restarted[0].ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerStartManyIsAtomic(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstPath := filepath.Join(root, "first.yaml")
	brokenPath := filepath.Join(root, "broken.yaml")
	var runs atomic.Int32
	manager := newTestManager(t, Options{
		Loader: func(path string) (*config.Config, error) {
			switch path {
			case firstPath:
				return testConfig(filepath.Join(root, "first.db")), nil
			case brokenPath:
				return nil, errors.New("invalid YAML")
			default:
				return nil, fmt.Errorf("unexpected path %s", path)
			}
		},
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			runs.Add(1)
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})

	_, err := manager.StartMany([]string{firstPath, brokenPath}, "")
	if err == nil || !strings.Contains(err.Error(), "invalid YAML") {
		t.Fatalf("StartMany error = %v", err)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("runners launched after failed validation = %d", got)
	}
	if got := manager.List(true); len(got) != 0 {
		t.Fatalf("instances retained after failed validation = %+v", got)
	}
	if got := manager.KnownQueues(); len(got) != 0 {
		t.Fatalf("queues retained after failed validation = %+v", got)
	}
}

func TestManagerStartContextOnlyBoundsPreflight(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "one.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32
	manager := newTestManager(t, Options{
		Loader: func(string) (*config.Config, error) {
			cancel()
			return testConfig(filepath.Join(root, "one.db")), nil
		},
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			runs.Add(1)
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})

	if _, err := manager.StartManyContext(ctx, []string{path}, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("StartManyContext error = %v, want context cancellation", err)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("runners launched after canceled preflight = %d", got)
	}
	if got := manager.List(true); len(got) != 0 {
		t.Fatalf("instances retained after canceled preflight = %+v", got)
	}
	if _, err := manager.StartManyContext(nil, []string{path}, ""); err == nil || !strings.Contains(err.Error(), "start context") {
		t.Fatalf("nil start context error = %v", err)
	}
}

func TestManagerStartGateWaitHonorsContext(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "blocked.yaml")
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			configPath: testConfig(filepath.Join(root, "blocked.db")),
		}),
	})
	manager.startGate <- struct{}{}
	t.Cleanup(func() { <-manager.startGate })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := manager.StartManyContext(ctx, []string{configPath}, "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StartManyContext() error = %v, want context deadline", err)
	}
}

func TestManagerShutdownUnblocksStartGateWaiter(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "blocked.yaml")
	loaded := make(chan struct{})
	manager := newTestManager(t, Options{
		Loader: func(string) (*config.Config, error) {
			close(loaded)
			return testConfig(filepath.Join(root, "blocked.db")), nil
		},
	})
	manager.startGate <- struct{}{}
	t.Cleanup(func() { <-manager.startGate })

	startDone := make(chan error, 1)
	go func() {
		_, err := manager.StartManyContext(context.Background(), []string{configPath}, "")
		startDone <- err
	}()
	<-loaded

	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(shutdownContext); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	select {
	case err := <-startDone:
		if !errors.Is(err, ErrShuttingDown) {
			t.Fatalf("StartManyContext() error = %v, want ErrShuttingDown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("start waiter did not unblock during shutdown")
	}
}

func TestManagerRejectsPathDatabaseAndNameConflicts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstPath := filepath.Join(root, "one", "same.yaml")
	secondPath := filepath.Join(root, "two", "same.yaml")
	thirdPath := filepath.Join(root, "three.yaml")
	firstDatabase := filepath.Join(root, "first.db")
	secondDatabase := filepath.Join(root, "second.db")
	configs := map[string]*config.Config{
		firstPath:  testConfig(firstDatabase),
		secondPath: testConfig(secondDatabase),
		thirdPath:  testConfig(firstDatabase),
	}
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, configs),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			<-ctx.Done()
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002", "000000000003", "000000000004"),
	})

	if _, err := manager.StartMany([]string{firstPath, secondPath}, "named"); err == nil || !strings.Contains(err.Error(), "only for one") {
		t.Fatalf("multi-config explicit-name error = %v", err)
	}
	first, err := manager.StartMany([]string{firstPath}, "chosen")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartMany([]string{filepath.Join(root, "one", ".", "same.yaml")}, ""); !errors.Is(err, ErrAlreadyActive) {
		t.Fatalf("active config conflict error = %v", err)
	}
	if _, err := manager.StartMany([]string{thirdPath}, ""); !errors.Is(err, ErrAlreadyActive) {
		t.Fatalf("active database conflict error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := manager.Stop(ctx, first[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartMany([]string{secondPath}, "chosen"); !errors.Is(err, ErrNameInUse) {
		t.Fatalf("retained name conflict error = %v", err)
	}
	restarted, err := manager.StartMany([]string{firstPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if restarted[0].Name != "same" {
		t.Fatalf("automatic name after explicit retained name = %q, want same", restarted[0].Name)
	}
	if _, err := manager.Stop(ctx, restarted[0].ID); err != nil {
		t.Fatal(err)
	}

	auto, err := manager.StartMany([]string{firstPath, secondPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := instanceNames(auto); !reflect.DeepEqual(got, []string{"same-2", "same-3"}) {
		t.Fatalf("unique automatic names = %v", got)
	}
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerRejectsHardLinkedDatabases(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstDatabase := filepath.Join(root, "first.db")
	aliasDatabase := filepath.Join(root, "alias.db")
	if err := os.WriteFile(firstDatabase, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(firstDatabase, aliasDatabase); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	firstPath := filepath.Join(root, "first.yaml")
	aliasPath := filepath.Join(root, "alias.yaml")
	var runs atomic.Int32
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			firstPath: testConfig(firstDatabase),
			aliasPath: testConfig(aliasDatabase),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			runs.Add(1)
			<-ctx.Done()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002"),
	})

	if _, err := manager.StartMany([]string{firstPath, aliasPath}, ""); err == nil || !strings.Contains(err.Error(), "same database") {
		t.Fatalf("hard-linked batch conflict error = %v", err)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("runners launched for hard-linked batch = %d", got)
	}

	first, err := manager.StartMany([]string{firstPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartMany([]string{aliasPath}, ""); !errors.Is(err, ErrAlreadyActive) {
		t.Fatalf("active hard-linked database conflict error = %v", err)
	}
	if _, err := manager.Stop(context.Background(), first[0].ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerRejectsHardLinkedConfigPaths(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstPath := filepath.Join(root, "first.yaml")
	aliasPath := filepath.Join(root, "alias.yaml")
	if err := os.WriteFile(firstPath, []byte("config"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(firstPath, aliasPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	var runs atomic.Int32
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			firstPath: testConfig(filepath.Join(root, "first.db")),
			aliasPath: testConfig(filepath.Join(root, "alias.db")),
		}),
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			runs.Add(1)
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002"),
	})
	if _, err := manager.StartMany([]string{firstPath, aliasPath}, ""); err == nil || !strings.Contains(err.Error(), "same path") {
		t.Fatalf("hard-linked config conflict error = %v", err)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("runners launched for hard-linked configs = %d", got)
	}
}

func TestManagerRejectsFreshDatabaseCaseAliasesOnInsensitiveFilesystem(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	upperDatabase := filepath.Join(root, "Queue.db")
	lowerDatabase := filepath.Join(root, "queue.db")
	equivalent, err := config.PathsEquivalent(upperDatabase, lowerDatabase)
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent {
		t.Skip("test filesystem is case-sensitive")
	}
	firstPath := filepath.Join(root, "first.yaml")
	secondPath := filepath.Join(root, "second.yaml")
	var runs atomic.Int32
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			firstPath:  testConfig(upperDatabase),
			secondPath: testConfig(lowerDatabase),
		}),
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			runs.Add(1)
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002"),
	})
	if _, err := manager.StartMany([]string{firstPath, secondPath}, ""); err == nil || !strings.Contains(err.Error(), "same database") {
		t.Fatalf("case-alias database conflict error = %v", err)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("runners launched for case-alias databases = %d", got)
	}
}

func TestManagerLoadsConfigThroughLexicalSymlinkButLocksCanonicalIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	targetDirectory := filepath.Join(root, "target")
	linkDirectory := filepath.Join(root, "links")
	for _, directory := range []string{targetDirectory, linkDirectory} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	targetPath := filepath.Join(targetDirectory, "source.yaml")
	configuration := `
database: {path: ./queue.db}
watches:
  - name: incoming
    path: ./incoming
    pipeline: [{name: inspect, program: /bin/true}]
`
	if err := os.WriteFile(targetPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(linkDirectory, "alias.yaml")
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Fatal(err)
	}

	manager := newTestManager(t, Options{
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			<-ctx.Done()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})
	instances, err := manager.StartMany([]string{linkPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := instances[0].ConfigPath, linkPath; got != want {
		t.Fatalf("display/load config path = %q, want lexical symlink %q", got, want)
	}
	if got, want := instances[0].DatabasePath, filepath.Join(linkDirectory, "queue.db"); got != want {
		t.Fatalf("database anchored at %q, want symlink directory path %q", got, want)
	}
	if _, err := manager.StartMany([]string{targetPath}, ""); !errors.Is(err, ErrAlreadyActive) {
		t.Fatalf("canonical config identity conflict error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := manager.Stop(ctx, instances[0].ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerKeepsNamesAndFullIDsDisjoint(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstPath := filepath.Join(root, "first.yaml")
	secondPath := filepath.Join(root, "second.yaml")
	thirdPath := filepath.Join(root, "third.yaml")
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			firstPath:  testConfig(filepath.Join(root, "first.db")),
			secondPath: testConfig(filepath.Join(root, "second.db")),
			thirdPath:  testConfig(filepath.Join(root, "third.db")),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			<-ctx.Done()
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator(
			"abcdefabcdef",
			"111111111111",
			"222222222222",
		),
	})
	first, err := manager.StartMany([]string{firstPath}, "111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StartMany([]string{secondPath}, first[0].ID); !errors.Is(err, ErrNameInUse) {
		t.Fatalf("name equal to retained full ID error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := manager.Stop(ctx, first[0].ID); err != nil {
		t.Fatal(err)
	}
	second, err := manager.StartMany([]string{secondPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := second[0].ID, "222222222222"; got != want {
		t.Fatalf("generated ID colliding with retained name = %q, want retry %q", got, want)
	}
	if _, err := manager.Stop(ctx, second[0].ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerAllowsIndependentExitAndFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	failPath := filepath.Join(root, "fail.yaml")
	waitPath := filepath.Join(root, "wait.yaml")
	failConfig := testConfig(filepath.Join(root, "fail.db"))
	waitConfig := testConfig(filepath.Join(root, "wait.db"))
	wantFailure := errors.New("watcher failed")
	waitStarted := make(chan struct{})
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{failPath: failConfig, waitPath: waitConfig}),
		Runner: func(ctx context.Context, cfg *config.Config, _ *slog.Logger) error {
			if cfg == failConfig {
				return wantFailure
			}
			close(waitStarted)
			<-ctx.Done()
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002"),
	})
	instances, err := manager.StartMany([]string{failPath, waitPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	failed, err := manager.Wait(ctx, instances[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != StateFailed || !strings.Contains(failed.Error, wantFailure.Error()) {
		t.Fatalf("failed instance = %+v", failed)
	}
	select {
	case <-waitStarted:
	case <-ctx.Done():
		t.Fatal("sibling runner did not start")
	}
	active := manager.List(false)
	if len(active) != 1 || active[0].ID != instances[1].ID {
		t.Fatalf("active sibling after failure = %+v", active)
	}
	if _, err := manager.Stop(ctx, instances[1].Name); err != nil {
		t.Fatal(err)
	}
}

func TestManagerBoundsTerminalInstanceRetention(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := []string{
		filepath.Join(root, "one.yaml"),
		filepath.Join(root, "two.yaml"),
		filepath.Join(root, "three.yaml"),
	}
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			paths[0]: testConfig(filepath.Join(root, "one.db")),
			paths[1]: testConfig(filepath.Join(root, "two.db")),
			paths[2]: testConfig(filepath.Join(root, "three.db")),
		}),
		Runner:            func(context.Context, *config.Config, *slog.Logger) error { return nil },
		IDGenerator:       sequenceIDGenerator("000000000001", "000000000002", "000000000003"),
		Clock:             sequenceClock(time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)),
		RetainedInstances: 2,
	})
	var firstID string
	for index, path := range paths {
		started, err := manager.StartMany([]string{path}, "")
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			firstID = started[0].ID
		}
		if _, err := manager.Wait(context.Background(), started[0].ID); err != nil {
			t.Fatal(err)
		}
	}
	if got := manager.List(true); len(got) != 2 {
		t.Fatalf("retained terminal instances = %d, want 2: %+v", len(got), got)
	}
	if _, err := manager.Wait(context.Background(), firstID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("evicted oldest instance error = %v, want ErrNotFound", err)
	}
}

func TestAttachedRemoveOnExitPreservesAttachmentAndReleasesName(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "ephemeral.yaml")
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			path: testConfig(filepath.Join(root, "ephemeral.db")),
		}),
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			return nil
		},
		IDGenerator: sequenceIDGenerator("00000000000c", "00000000000d"),
	})

	first, attachment, err := manager.startAttachedContext(context.Background(), path, "ephemeral", true)
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.Cancel()
	finished, err := attachment.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if finished.ID != first.ID || finished.Name != "ephemeral" || finished.State != StateExited || finished.FinishedAt == nil {
		t.Fatalf("remove-on-exit attachment result = %+v", finished)
	}
	if instances := manager.List(true); len(instances) != 0 {
		t.Fatalf("instances retained after remove-on-exit = %+v", instances)
	}
	if _, err := manager.Wait(context.Background(), first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("registry lookup after remove-on-exit error = %v, want ErrNotFound", err)
	}
	if queues := manager.KnownQueues(); len(queues) != 1 || queues[0].ConfigPath != path {
		t.Fatalf("known queues after remove-on-exit = %+v", queues)
	}

	restarted, err := manager.StartMany([]string{path}, "ephemeral")
	if err != nil {
		t.Fatalf("reuse removed instance name: %v", err)
	}
	if len(restarted) != 1 || restarted[0].ID != "00000000000d" || restarted[0].Name != "ephemeral" {
		t.Fatalf("restarted instance = %+v", restarted)
	}
	if _, err := manager.Wait(context.Background(), restarted[0].ID); err != nil {
		t.Fatal(err)
	}

	finishedAgain, err := attachment.Wait(context.Background())
	if err != nil || finishedAgain.ID != first.ID || finishedAgain.State != StateExited {
		t.Fatalf("attachment after name reuse = %+v, %v", finishedAgain, err)
	}
}

func TestAttachedStartSurvivesTerminalRegistryEviction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstPath := filepath.Join(root, "first.yaml")
	secondPath := filepath.Join(root, "second.yaml")
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			firstPath:  testConfig(filepath.Join(root, "first.db")),
			secondPath: testConfig(filepath.Join(root, "second.db")),
		}),
		Runner: func(_ context.Context, cfg *config.Config, logger *slog.Logger) error {
			logger.Info("runner complete", "database", cfg.Database.Path)
			return nil
		},
		IDGenerator:       sequenceIDGenerator("000000000001", "000000000002"),
		RetainedInstances: 1,
	})

	first, attachment, err := manager.StartAttachedContext(context.Background(), firstPath, "")
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.Cancel()
	if _, err := attachment.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := manager.StartMany([]string{secondPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Wait(context.Background(), second[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Wait(context.Background(), first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("registry lookup after eviction error = %v, want ErrNotFound", err)
	}

	finished, err := attachment.Wait(context.Background())
	if err != nil || finished.ID != first.ID || finished.State != StateExited {
		t.Fatalf("attached wait after eviction = %+v, %v", finished, err)
	}
	if !readUntilContains(t, attachment.Lines(), "runner complete", time.Second) {
		t.Fatal("attachment lost logs after registry eviction")
	}
}

func TestManagerStopDoesNotHideJoinedOperationalFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "joined.yaml")
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{path: testConfig(filepath.Join(root, "joined.db"))}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			<-ctx.Done()
			return errors.Join(ctx.Err(), errors.New("database close failed"))
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})
	instances, err := manager.StartMany([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopped, err := manager.Stop(ctx, instances[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != StateFailed || !strings.Contains(stopped.Error, "database close failed") {
		t.Fatalf("joined failure was hidden: %+v", stopped)
	}
}

func TestManagerWaitAndSelectorErrors(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	onePath := filepath.Join(root, "one.yaml")
	twoPath := filepath.Join(root, "two.yaml")
	releases := map[string]chan struct{}{
		filepath.Join(root, "one.db"): make(chan struct{}),
		filepath.Join(root, "two.db"): make(chan struct{}),
	}
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			onePath: testConfig(filepath.Join(root, "one.db")),
			twoPath: testConfig(filepath.Join(root, "two.db")),
		}),
		Runner: func(_ context.Context, cfg *config.Config, _ *slog.Logger) error {
			<-releases[cfg.Database.Path]
			return nil
		},
		IDGenerator: sequenceIDGenerator("abcdef000001", "abcdef000002"),
	})
	instances, err := manager.StartMany([]string{onePath, twoPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Wait(context.Background(), "abcdef"); !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("ambiguous Wait error = %v", err)
	}
	if _, err := manager.Wait(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Wait error = %v", err)
	}
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := manager.Wait(timeout, instances[0].Name); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait timeout error = %v", err)
	}
	close(releases[filepath.Join(root, "one.db")])
	finished, err := manager.Wait(context.Background(), "abcdef000001")
	if err != nil || finished.State != StateExited {
		t.Fatalf("Wait completed = %+v, %v", finished, err)
	}
	if _, err := manager.Stop(context.Background(), finished.Name); !errors.Is(err, ErrNotActive) {
		t.Fatalf("stop exited error = %v", err)
	}
	close(releases[filepath.Join(root, "two.db")])
	if _, err := manager.Wait(context.Background(), instances[1].ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerShutdownCancelsWaitsAndRejectsStarts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	paths := []string{filepath.Join(root, "one.yaml"), filepath.Join(root, "two.yaml")}
	startedRuns := make(chan struct{}, 2)
	stoppedRuns := make(chan struct{}, 2)
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{
			paths[0]: testConfig(filepath.Join(root, "one.db")),
			paths[1]: testConfig(filepath.Join(root, "two.db")),
		}),
		Runner: func(ctx context.Context, _ *config.Config, _ *slog.Logger) error {
			startedRuns <- struct{}{}
			<-ctx.Done()
			stoppedRuns <- struct{}{}
			return ctx.Err()
		},
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002"),
	})
	if _, err := manager.StartMany(paths, ""); err != nil {
		t.Fatal(err)
	}
	waitForSignals(t, startedRuns, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	waitForSignals(t, stoppedRuns, 2)
	for _, instance := range manager.List(true) {
		if instance.State != StateExited {
			t.Fatalf("instance after shutdown = %+v", instance)
		}
	}
	if _, err := manager.StartMany([]string{paths[0]}, ""); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("start after shutdown error = %v", err)
	}
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown error = %v", err)
	}
}

func TestManagerShutdownHonorsWaitContext(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "stubborn.yaml")
	release := make(chan struct{})
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{path: testConfig(filepath.Join(root, "stubborn.db"))}),
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			<-release
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})
	if _, err := manager.StartMany([]string{path}, ""); err != nil {
		t.Fatal(err)
	}
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := manager.Shutdown(timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown timeout error = %v", err)
	}
	close(release)
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerShutdownWaitsForTerminalFinalization(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "finalizing.yaml")
	runnerRelease := make(chan struct{})
	terminalLogBlocked := make(chan struct{})
	terminalLogRelease := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(terminalLogRelease) })
	baseLogger := slog.New(slog.NewTextHandler(&terminalBlockingWriter{
		blocked: terminalLogBlocked,
		release: terminalLogRelease,
	}, nil))
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{path: testConfig(filepath.Join(root, "finalizing.db"))}),
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			<-runnerRelease
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
		Logger:      baseLogger,
	})
	if _, err := manager.StartMany([]string{path}, ""); err != nil {
		t.Fatal(err)
	}
	close(runnerRelease)
	select {
	case <-terminalLogBlocked:
	case <-time.After(time.Second):
		t.Fatal("runner did not enter terminal finalization")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- manager.Shutdown(ctx)
	}()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before finalization completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(terminalLogRelease) })
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestManagerLogsAreRetainedLiveAndForwarded(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "logged.yaml")
	ready := make(chan struct{})
	release := make(chan struct{})
	var daemonLogs bytes.Buffer
	baseLogger := slog.New(slog.NewTextHandler(&daemonLogs, nil))
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{path: testConfig(filepath.Join(root, "logged.db"))}),
		Runner: func(_ context.Context, _ *config.Config, logger *slog.Logger) error {
			logger.Info("retained record", "phase", 1)
			close(ready)
			<-release
			logger.Info("live record", "phase", 2)
			return nil
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
		Logger:      baseLogger,
		LogCapacity: 16,
	})
	instances, err := manager.StartMany([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("runner did not write retained log")
	}
	subscription, err := manager.Logs(instances[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()
	if !readUntilContains(t, subscription.Lines(), "config_hash="+instances[0].ConfigHash, time.Second) {
		t.Fatal("subscription startup log did not contain the config hash")
	}
	if !readUntilContains(t, subscription.Lines(), "retained record", time.Second) {
		t.Fatal("subscription did not replay retained record")
	}
	close(release)
	if !readUntilContains(t, subscription.Lines(), "live record", time.Second) {
		t.Fatal("subscription did not receive live record")
	}
	finished, err := manager.Wait(context.Background(), instances[0].ID)
	if err != nil || finished.State != StateExited {
		t.Fatalf("Wait = %+v, %v", finished, err)
	}
	for range subscription.Lines() {
	}
	if output := daemonLogs.String(); !strings.Contains(output, "retained record") || !strings.Contains(output, "live record") {
		t.Fatalf("daemon logger output = %q", output)
	}
	retained, err := manager.Logs(instances[0].ID[:4])
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Cancel()
	if !readUntilContains(t, retained.Lines(), "instance exited", time.Second) {
		t.Fatal("completed subscription did not contain terminal log")
	}
}

func TestManagerConfigHashTracksEffectiveConfigAcrossRuns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, "tracked.yaml")
	base := testConfig(filepath.Join(root, "tracked.db"))
	base.Queue.Workers = 1
	same := *base
	changed := *base
	changed.Queue.Workers = 2
	configs := []*config.Config{base, &same, &changed}

	var (
		loaderMu    sync.Mutex
		loaderIndex int
		logOutput   bytes.Buffer
	)
	manager := newTestManager(t, Options{
		Loader: func(loadedPath string) (*config.Config, error) {
			if loadedPath != path {
				return nil, fmt.Errorf("unexpected config path %q", loadedPath)
			}
			loaderMu.Lock()
			defer loaderMu.Unlock()
			if loaderIndex >= len(configs) {
				return nil, errors.New("config sequence exhausted")
			}
			cfg := configs[loaderIndex]
			loaderIndex++
			return cfg, nil
		},
		Runner:      func(context.Context, *config.Config, *slog.Logger) error { return nil },
		IDGenerator: sequenceIDGenerator("000000000001", "000000000002", "000000000003"),
		Logger:      slog.New(slog.NewJSONHandler(&logOutput, nil)),
	})

	instances := make([]Instance, 0, len(configs))
	for range configs {
		started, err := manager.StartMany([]string{path}, "")
		if err != nil {
			t.Fatal(err)
		}
		finished, err := manager.Wait(context.Background(), started[0].ID)
		if err != nil {
			t.Fatal(err)
		}
		instances = append(instances, finished)
	}

	if instances[0].ID == instances[1].ID {
		t.Fatalf("separate runs reused instance ID %q", instances[0].ID)
	}
	if got, want := instances[1].ConfigHash, instances[0].ConfigHash; got != want {
		t.Fatalf("unchanged config hashes differ: %q != %q", got, want)
	}
	if instances[2].ConfigHash == instances[0].ConfigHash {
		t.Fatalf("changed config retained hash %q", instances[2].ConfigHash)
	}
	for _, instance := range instances {
		if len(instance.ConfigHash) != 64 {
			t.Fatalf("config hash length = %d, want 64: %q", len(instance.ConfigHash), instance.ConfigHash)
		}
		if _, err := hex.DecodeString(instance.ConfigHash); err != nil {
			t.Fatalf("config hash is not hexadecimal: %q: %v", instance.ConfigHash, err)
		}
	}

	startupHashes := make(map[string]string, len(instances))
	for _, line := range strings.Split(strings.TrimSpace(logOutput.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log record %q: %v", line, err)
		}
		if record["msg"] == "instance running" {
			instanceID, _ := record["instance_id"].(string)
			configHash, _ := record["config_hash"].(string)
			startupHashes[instanceID] = configHash
		}
	}
	for _, instance := range instances {
		if got := startupHashes[instance.ID]; got != instance.ConfigHash {
			t.Fatalf("startup log config hash for %s = %q, want %q", instance.ID, got, instance.ConfigHash)
		}
	}
}

func TestManagerRecoversRunnerPanic(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "panic.yaml")
	manager := newTestManager(t, Options{
		Loader: mappedLoader(t, map[string]*config.Config{path: testConfig(filepath.Join(root, "panic.db"))}),
		Runner: func(context.Context, *config.Config, *slog.Logger) error {
			panic("boom")
		},
		IDGenerator: sequenceIDGenerator("000000000001"),
	})
	instances, err := manager.StartMany([]string{path}, "")
	if err != nil {
		t.Fatal(err)
	}
	finished, err := manager.Wait(context.Background(), instances[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.State != StateFailed || !strings.Contains(finished.Error, "runner panic: boom") {
		t.Fatalf("panic state = %+v", finished)
	}
}

func TestManagerRejectsInvalidGeneratedIDAndOptions(t *testing.T) {
	t.Parallel()
	for name, options := range map[string]Options{
		"log capacity":       {LogCapacity: -1},
		"log byte capacity":  {MaxLogBytes: -1},
		"retained instances": {RetainedInstances: -1},
	} {
		if _, err := NewManager(options); err == nil {
			t.Errorf("NewManager accepted negative %s", name)
		}
	}
	root := t.TempDir()
	path := filepath.Join(root, "one.yaml")
	manager := newTestManager(t, Options{
		Loader:      mappedLoader(t, map[string]*config.Config{path: testConfig(filepath.Join(root, "one.db"))}),
		Runner:      func(context.Context, *config.Config, *slog.Logger) error { return nil },
		IDGenerator: sequenceIDGenerator("not-an-id"),
	})
	if _, err := manager.StartMany([]string{path}, ""); err == nil || !strings.Contains(err.Error(), "invalid 12-hex") {
		t.Fatalf("invalid ID error = %v", err)
	}
	if got := manager.List(true); len(got) != 0 {
		t.Fatalf("invalid ID retained instances = %+v", got)
	}
}

func newTestManager(t *testing.T, options Options) *Manager {
	t.Helper()
	manager, err := NewManager(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	})
	return manager
}

func testConfig(databasePath string) *config.Config {
	return &config.Config{Database: config.DatabaseConfig{Path: databasePath}}
}

func mappedLoader(t *testing.T, configs map[string]*config.Config) Loader {
	t.Helper()
	canonical := make(map[string]*config.Config, len(configs))
	for path, cfg := range configs {
		resolved, err := canonicalPath(path)
		if err != nil {
			t.Fatal(err)
		}
		canonical[resolved] = cfg
	}
	return func(path string) (*config.Config, error) {
		cfg, exists := canonical[path]
		if !exists {
			return nil, fmt.Errorf("unexpected config path %q", path)
		}
		return cfg, nil
	}
}

func sequenceIDGenerator(ids ...string) IDGenerator {
	var (
		mu    sync.Mutex
		index int
	)
	return func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if index >= len(ids) {
			return "", errors.New("ID sequence exhausted")
		}
		id := ids[index]
		index++
		return id, nil
	}
}

func sequenceClock(start time.Time) Clock {
	var (
		mu   sync.Mutex
		tick time.Duration
	)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		value := start.Add(tick)
		tick += time.Second
		return value
	}
}

type terminalBlockingWriter struct {
	blocked chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (writer *terminalBlockingWriter) Write(value []byte) (int, error) {
	if bytes.Contains(value, []byte("instance exited")) {
		writer.once.Do(func() { close(writer.blocked) })
		<-writer.release
	}
	return len(value), nil
}

func waitForRunnerStarts(t *testing.T, starts <-chan *config.Config, count int) {
	t.Helper()
	for range count {
		select {
		case <-starts:
		case <-time.After(time.Second):
			t.Fatal("runner did not start")
		}
	}
}

func waitForSignals(t *testing.T, signals <-chan struct{}, count int) {
	t.Helper()
	for range count {
		select {
		case <-signals:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for signal")
		}
	}
}

func instanceNames(instances []Instance) []string {
	names := make([]string, len(instances))
	for index, instance := range instances {
		names[index] = instance.Name
	}
	return names
}

func readUntilContains(t *testing.T, lines <-chan string, text string, timeout time.Duration) bool {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case line, open := <-lines:
			if !open {
				return false
			}
			if strings.Contains(line, text) {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}
