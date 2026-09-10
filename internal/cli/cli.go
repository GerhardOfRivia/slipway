// Package cli implements onderzeeer's management command line interface.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/GerhardOfRivia/onderzeeer/internal/config"
	"github.com/GerhardOfRivia/onderzeeer/internal/queue"
)

type usageError struct {
	message string
}

func (e usageError) Error() string { return e.message }

type loadedConfig struct {
	path   string
	config *config.Config
}

type configuredStore struct {
	path   string
	config *config.Config
	store  queueReader
}

type queueReader interface {
	Close() error
	Counts(context.Context) (queue.QueueCounts, error)
	ListJobs(context.Context, queue.JobFilter) ([]queue.Job, error)
	GetJob(context.Context, int64) (*queue.Job, error)
	ListRuns(context.Context, int64) ([]queue.Run, error)
	ListCommands(context.Context, int64) ([]queue.CommandExecution, error)
}

type listedJob struct {
	configPath string
	job        queue.Job
}

// Run executes a CLI invocation and returns a process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	return RunVersion(args, stdout, stderr, "dev")
}

// RunVersion executes a CLI invocation using version as the displayed build
// version. Run exists as a convenient entry point for callers that do not
// inject build metadata.
func RunVersion(args []string, stdout, stderr io.Writer, version string) int {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	var err error
	switch args[0] {
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	case "version", "-v", "-version", "--version":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "onderzeeer: version does not accept arguments")
			return 2
		}
		fmt.Fprintf(stdout, "onderzeeer %s\n", version)
		return 0
	case "check":
		err = checkCommand(args[1:], stdout, stderr)
	case "parse":
		err = parseCommand(args[1:], stdout, stderr)
	case "test":
		err = testCommand(args[1:], stdout, stderr)
	case "start":
		err = startCommand(args[1:], stdout, stderr)
	case "ps":
		err = psCommand(args[1:], stdout, stderr)
	case "stop":
		err = stopCommand(args[1:], stdout, stderr)
	case "status":
		err = statusCommand(args[1:], stdout, stderr)
	case "queue":
		err = queueCommand(args[1:], stdout, stderr)
	case "jobs":
		err = jobsCommand(args[1:], stdout, stderr)
	case "job":
		err = jobCommand(args[1:], stdout, stderr)
	case "logs":
		err = logsCommand(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "onderzeeer: unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	var usage usageError
	if errors.As(err, &usage) {
		fmt.Fprintln(stderr, "onderzeeer:", usage.message)
		return 2
	}
	fmt.Fprintln(stderr, "onderzeeer:", err)
	return 1
}

func statusCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("status", stderr, "onderzeeer status <config>")
	inspection := inspectionFlags(flags)
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireArguments(flags, "config path"); err != nil {
		return err
	}

	stores, err := inspection.open(flags.Arg(0))
	if err != nil {
		return err
	}
	defer closeStores(stores)
	ctx := context.Background()
	w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	if len(stores) == 1 {
		counts, err := stores[0].store.Counts(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "CONFIG\t%s\n", stores[0].path)
		fmt.Fprintf(w, "DATABASE\t%s\n", stores[0].config.Database.Path)
		fmt.Fprintf(w, "TOTAL\t%d\n", counts.Total)
		fmt.Fprintf(w, "QUEUED\t%d\n", counts.Queued)
		fmt.Fprintf(w, "RUNNING\t%d\n", counts.Running)
		fmt.Fprintf(w, "SUCCEEDED\t%d\n", counts.Succeeded)
		fmt.Fprintf(w, "FAILED\t%d\n", counts.Failed)
		return w.Flush()
	}

	fmt.Fprintln(w, "CONFIG\tDATABASE\tTOTAL\tQUEUED\tRUNNING\tSUCCEEDED\tFAILED")
	var total queue.QueueCounts
	for _, source := range stores {
		counts, err := source.store.Counts(ctx)
		if err != nil {
			return fmt.Errorf("status %s: %w", source.path, err)
		}
		total.Total += counts.Total
		total.Queued += counts.Queued
		total.Running += counts.Running
		total.Succeeded += counts.Succeeded
		total.Failed += counts.Failed
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%d\n",
			source.path, source.config.Database.Path, counts.Total, counts.Queued,
			counts.Running, counts.Succeeded, counts.Failed)
	}
	fmt.Fprintf(w, "TOTAL\t-\t%d\t%d\t%d\t%d\t%d\n",
		total.Total, total.Queued, total.Running, total.Succeeded, total.Failed)
	return w.Flush()
}

func queueCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("queue", stderr, "onderzeeer queue <config> [--watch name] [--limit n]")
	inspection := inspectionFlags(flags)
	watchName := flags.String("watch", "", "filter by watch name")
	limit := flags.Int("limit", 100, "maximum jobs to display")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireArguments(flags, "config path"); err != nil {
		return err
	}
	if *limit <= 0 {
		return usageError{message: "queue --limit must be greater than zero"}
	}

	stores, err := inspection.open(flags.Arg(0))
	if err != nil {
		return err
	}
	defer closeStores(stores)
	ctx := context.Background()
	jobs := make([]listedJob, 0)
	for _, source := range stores {
		queued, err := source.store.ListJobs(ctx, queue.JobFilter{Status: queue.StatusQueued, WatchName: *watchName, Limit: *limit})
		if err != nil {
			return fmt.Errorf("list queue %s: %w", source.path, err)
		}
		running, err := source.store.ListJobs(ctx, queue.JobFilter{Status: queue.StatusRunning, WatchName: *watchName, Limit: *limit})
		if err != nil {
			return fmt.Errorf("list queue %s: %w", source.path, err)
		}
		jobs = appendListedJobs(jobs, source.path, queued)
		jobs = appendListedJobs(jobs, source.path, running)
	}
	sortListedJobs(jobs)
	if len(jobs) > *limit {
		jobs = jobs[:*limit]
	}
	return printJobs(stdout, jobs, len(stores) > 1)
}

func jobsCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("jobs", stderr, "onderzeeer jobs <config> [--status status] [--watch name] [--limit n]")
	inspection := inspectionFlags(flags)
	statusText := flags.String("status", "", "queued, running, succeeded, or failed")
	watchName := flags.String("watch", "", "filter by watch name")
	limit := flags.Int("limit", 100, "maximum jobs to display")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireArguments(flags, "config path"); err != nil {
		return err
	}
	if *limit <= 0 {
		return usageError{message: "jobs --limit must be greater than zero"}
	}
	status, err := parseStatus(*statusText)
	if err != nil {
		return err
	}

	stores, err := inspection.open(flags.Arg(0))
	if err != nil {
		return err
	}
	defer closeStores(stores)
	ctx := context.Background()
	jobs := make([]listedJob, 0)
	for _, source := range stores {
		found, err := source.store.ListJobs(ctx, queue.JobFilter{
			Status:    status,
			WatchName: *watchName,
			Limit:     *limit,
		})
		if err != nil {
			return fmt.Errorf("list jobs %s: %w", source.path, err)
		}
		jobs = appendListedJobs(jobs, source.path, found)
	}
	sortListedJobs(jobs)
	if len(jobs) > *limit {
		jobs = jobs[:*limit]
	}
	return printJobs(stdout, jobs, len(stores) > 1)
}

func jobCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("job", stderr, "onderzeeer job <config> <id>")
	inspection := inspectionFlags(flags)
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	id, err := oneJobID(flags)
	if err != nil {
		return err
	}

	stores, err := inspection.open(flags.Arg(0))
	if err != nil {
		return err
	}
	defer closeStores(stores)
	ctx := context.Background()
	source, job, err := findJob(ctx, stores, id)
	if err != nil {
		return err
	}
	runs, err := source.store.ListRuns(ctx, id)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "Job %d\n", job.ID)
	if len(stores) > 1 {
		fmt.Fprintf(stdout, "Config: %s\n", source.path)
	}
	printJobFields(stdout, job)
	if len(runs) == 0 {
		fmt.Fprintln(stdout, "\nNo runs recorded.")
		return nil
	}
	fmt.Fprintln(stdout, "\nRuns")
	for _, run := range runs {
		fmt.Fprintf(stdout, "  Run %d (id=%d)  %s  started=%s  finished=%s\n",
			run.Attempt, run.ID, run.Status, formatTime(run.StartedAt), formatOptionalTime(run.FinishedAt))
		if run.Error != "" {
			fmt.Fprintf(stdout, "    error: %s\n", run.Error)
		}
		commands, err := source.store.ListCommands(ctx, run.ID)
		if err != nil {
			return err
		}
		for _, command := range commands {
			fmt.Fprintf(stdout, "    Command %d (id=%d)  %s  %s\n",
				command.Sequence, command.ID, command.Status, formatInvocation(command.Program, command.Args))
			fmt.Fprintf(stdout, "      started=%s  finished=%s  exit=%s  timeout=%s\n",
				formatTime(command.StartedAt), formatOptionalTime(command.FinishedAt), formatExitCode(command.ExitCode), command.Timeout)
			if command.Error != "" {
				fmt.Fprintf(stdout, "      error: %s\n", command.Error)
			}
		}
	}
	return nil
}

func logsCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("logs", stderr, "onderzeeer logs <config> <id>")
	inspection := inspectionFlags(flags)
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	id, err := oneJobID(flags)
	if err != nil {
		return err
	}

	stores, err := inspection.open(flags.Arg(0))
	if err != nil {
		return err
	}
	defer closeStores(stores)
	ctx := context.Background()
	source, _, err := findJob(ctx, stores, id)
	if err != nil {
		return err
	}
	runs, err := source.store.ListRuns(ctx, id)
	if err != nil {
		return err
	}
	for _, run := range runs {
		commands, err := source.store.ListCommands(ctx, run.ID)
		if err != nil {
			return err
		}
		for _, command := range commands {
			fmt.Fprintf(stdout, "== Run %d / Command %d: %s (%s) ==\n", run.Attempt, command.Sequence, command.Name, command.Status)
			fmt.Fprintln(stdout, "-- stdout --")
			writeLog(stdout, command.Stdout)
			fmt.Fprintln(stdout, "-- stderr --")
			writeLog(stdout, command.Stderr)
		}
	}
	return nil
}

func loadConfigs(selection string) ([]loadedConfig, error) {
	paths, err := discoverConfigs(selection)
	if err != nil {
		return nil, err
	}
	return loadConfigPaths(paths)
}

func discoverConfigs(selection string) ([]string, error) {
	if strings.TrimSpace(selection) == "" {
		return nil, usageError{message: "config path is required"}
	}
	return config.Discover(selection)
}

func loadConfigPaths(paths []string) ([]loadedConfig, error) {
	loaded := make([]loadedConfig, 0, len(paths))
	for _, path := range paths {
		cfg, err := config.Load(path)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", path, err)
		}
		loaded = append(loaded, loadedConfig{path: path, config: cfg})
	}
	return loaded, nil
}

func openStores(selection string) ([]configuredStore, error) {
	loaded, err := loadConfigs(selection)
	if err != nil {
		return nil, err
	}
	type databaseOwner struct {
		configPath   string
		databasePath string
	}
	databaseOwners := make([]databaseOwner, 0, len(loaded))
	for _, item := range loaded {
		databasePath, err := config.CanonicalDatabasePath(item.config.Database.Path)
		if err != nil {
			return nil, fmt.Errorf("resolve database path for %s: %w", item.path, err)
		}
		for _, owner := range databaseOwners {
			equivalent, err := config.PathsEquivalent(owner.databasePath, databasePath)
			if err != nil {
				return nil, fmt.Errorf("compare database paths for %s and %s: %w", owner.configPath, item.path, err)
			}
			if equivalent {
				return nil, fmt.Errorf("configs %s and %s use the same database %s; pass one config path or use distinct databases", owner.configPath, item.path, databasePath)
			}
		}
		databaseOwners = append(databaseOwners, databaseOwner{configPath: item.path, databasePath: databasePath})
	}

	stores := make([]configuredStore, 0, len(loaded))
	for _, item := range loaded {
		store, err := queue.OpenReadOnly(item.config.Database.Path)
		if err != nil {
			closeStores(stores)
			return nil, fmt.Errorf("open database for %s: %w", item.path, err)
		}
		stores = append(stores, configuredStore{path: item.path, config: item.config, store: store})
	}
	return stores, nil
}

func closeStores(stores []configuredStore) {
	for i := range stores {
		_ = stores[i].store.Close()
	}
}

func findJob(ctx context.Context, stores []configuredStore, id int64) (*configuredStore, *queue.Job, error) {
	var (
		matchedStore *configuredStore
		matchedJob   *queue.Job
	)
	for i := range stores {
		job, err := stores[i].store.GetJob(ctx, id)
		if errors.Is(err, queue.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("find job in %s: %w", stores[i].path, err)
		}
		if matchedStore != nil {
			return nil, nil, fmt.Errorf("job %d exists in both %s and %s; pass its config path explicitly", id, matchedStore.path, stores[i].path)
		}
		matchedStore = &stores[i]
		matchedJob = job
	}
	if matchedStore == nil {
		return nil, nil, fmt.Errorf("job %d: %w", id, queue.ErrNotFound)
	}
	return matchedStore, matchedJob, nil
}

func appendListedJobs(destination []listedJob, configPath string, jobs []queue.Job) []listedJob {
	for _, job := range jobs {
		destination = append(destination, listedJob{configPath: configPath, job: job})
	}
	return destination
}

func sortListedJobs(jobs []listedJob) {
	sort.Slice(jobs, func(i, j int) bool {
		if !jobs[i].job.CreatedAt.Equal(jobs[j].job.CreatedAt) {
			return jobs[i].job.CreatedAt.After(jobs[j].job.CreatedAt)
		}
		if jobs[i].configPath != jobs[j].configPath {
			return jobs[i].configPath < jobs[j].configPath
		}
		return jobs[i].job.ID > jobs[j].job.ID
	})
}

func newFlagSet(name string, output io.Writer, usage string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage:", usage)
		flags.PrintDefaults()
		switch name {
		case "start", "ps", "stop":
			fmt.Fprintln(output, "\nRequires a running onderzeeerd. Start it separately with: onderzeeerd")
		case "test":
			fmt.Fprintln(output, "\nRuns in the foreground with its own queue database. Does not contact onderzeeerd.")
		case "check", "parse":
			fmt.Fprintln(output, "\nNo onderzeeerd required. Does not contact the daemon or execute pipeline commands.")
		case "status", "queue", "jobs", "job", "logs":
			fmt.Fprintln(output, "\nSelect a managed instance by name, ID, or config path. Requires onderzeeerd.")
			fmt.Fprintln(output, "Use --local with a config path to inspect a standalone run's database.")
		case "onderzeeerd":
			fmt.Fprintln(output, "\nStarts the daemon; no existing onderzeeerd is required.")
		}
	}
	return flags
}

// parseFlags accepts options before or after positional arguments. A standalone
// -- ends option parsing, including for paths or names that begin with a dash.
func parseFlags(flags *flag.FlagSet, args []string) error {
	var options, positional []string
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if argument == "--" {
			positional = append(positional, args[index+1:]...)
			break
		}
		if argument == "-" || !strings.HasPrefix(argument, "-") {
			positional = append(positional, argument)
			continue
		}
		options = append(options, argument)
		name := strings.TrimPrefix(strings.TrimPrefix(argument, "-"), "-")
		name, _, hasValue := strings.Cut(name, "=")
		option := flags.Lookup(name)
		if option == nil {
			// Let flag report unknown options and handle -h/--help normally.
			return flags.Parse(options)
		}
		boolean, isBoolean := option.Value.(interface{ IsBoolFlag() bool })
		if !hasValue && !(isBoolean && boolean.IsBoolFlag()) {
			if index+1 == len(args) {
				return flags.Parse(options)
			}
			index++
			options = append(options, args[index])
		}
	}
	return flags.Parse(append(append(options, "--"), positional...))
}

func requireArguments(flags *flag.FlagSet, names ...string) error {
	if flags.NArg() > len(names) {
		noun := "arguments"
		if len(names) == 1 {
			noun = "argument"
		}
		return usageError{message: fmt.Sprintf("%s expects %d positional %s (%s)", flags.Name(), len(names), noun, strings.Join(names, ", "))}
	}
	for index, name := range names {
		if strings.TrimSpace(flags.Arg(index)) == "" {
			return usageError{message: name + " is required"}
		}
	}
	return nil
}

func oneJobID(flags *flag.FlagSet) (int64, error) {
	if err := requireArguments(flags, "config path", "job ID"); err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(flags.Arg(1), 10, 64)
	if err != nil || id <= 0 {
		return 0, usageError{message: fmt.Sprintf("invalid job ID %q", flags.Arg(1))}
	}
	return id, nil
}

func parseStatus(value string) (queue.Status, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	status := queue.Status(strings.ToUpper(strings.TrimSpace(value)))
	switch status {
	case queue.StatusQueued, queue.StatusRunning, queue.StatusSucceeded, queue.StatusFailed:
		return status, nil
	default:
		return "", usageError{message: fmt.Sprintf("unknown job status %q", value)}
	}
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, usageError{message: fmt.Sprintf("unknown log level %q", value)}
	}
}

func printJobs(output io.Writer, jobs []listedJob, showConfig bool) error {
	w := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if showConfig {
		fmt.Fprintln(w, "CONFIG\tID\tSTATUS\tWATCH\tATTEMPT\tAVAILABLE\tPATH")
	} else {
		fmt.Fprintln(w, "ID\tSTATUS\tWATCH\tATTEMPT\tAVAILABLE\tPATH")
	}
	for _, listed := range jobs {
		job := listed.job
		available := "-"
		if job.Status == queue.StatusQueued {
			available = formatTime(job.AvailableAt)
		}
		if showConfig {
			fmt.Fprintf(w, "%s\t", listed.configPath)
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d/%d\t%s\t%s\n",
			job.ID, job.Status, job.WatchName, job.Attempts, job.MaxRetries+1, available, strconv.Quote(job.Path))
	}
	return w.Flush()
}

func printJobFields(output io.Writer, job *queue.Job) {
	w := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "Status:\t%s\n", job.Status)
	fmt.Fprintf(w, "Watch:\t%s\n", job.WatchName)
	fmt.Fprintf(w, "File:\t%s\n", strconv.Quote(job.Path))
	fmt.Fprintf(w, "Fingerprint:\t%s\n", job.Fingerprint)
	fmt.Fprintf(w, "Attempts:\t%d/%d\n", job.Attempts, job.MaxRetries+1)
	fmt.Fprintf(w, "Available:\t%s\n", formatTime(job.AvailableAt))
	fmt.Fprintf(w, "Created:\t%s\n", formatTime(job.CreatedAt))
	fmt.Fprintf(w, "Updated:\t%s\n", formatTime(job.UpdatedAt))
	if job.LastError != "" {
		fmt.Fprintf(w, "Last error:\t%s\n", job.LastError)
	}
	_ = w.Flush()
}

func formatInvocation(program string, args []string) string {
	if args == nil {
		args = []string{}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		encoded = []byte("[]")
	}
	return strconv.Quote(program) + " " + string(encoded)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return formatTime(*value)
}

func formatExitCode(value *int) string {
	if value == nil {
		return "-"
	}
	return strconv.Itoa(*value)
}

func writeLog(output io.Writer, value string) {
	io.WriteString(output, value)
	if !strings.HasSuffix(value, "\n") {
		fmt.Fprintln(output)
	}
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, `onderzeeer watches files and runs durable command pipelines.

Usage:

No onderzeeerd required:
  onderzeeer version
  onderzeeer check [--raw] <config>
  onderzeeer parse [--name name] -- <program> [argument ...]
  onderzeeer test <config>

Requires a running onderzeeerd:
  onderzeeer start <config-or-instance> [name] [--socket path]
  onderzeeer ps [--all] [--socket path]
  onderzeeer stop [--socket path] <id-or-name> [id-or-name ...]
  onderzeeer status <instance-or-config> [--socket path]
  onderzeeer queue <instance-or-config> [--socket path]
  onderzeeer jobs <instance-or-config> [--status status] [--watch name] [--socket path]
  onderzeeer job <instance-or-config> <id> [--socket path]
  onderzeeer logs <instance-or-config> <id> [--socket path]
  Start the daemon separately with: onderzeeerd`)
}
