// Package config loads and validates slipway's YAML configuration.
package config

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultDatabasePath = "./slipway.db"
	defaultRetryDelay   = 10 * time.Second
	defaultSettleFor    = time.Second
)

// ExecutorType selects how a pipeline step's host-side executable is chosen.
// Container executor arguments are passed directly to the selected runtime CLI
// so slipway does not need to duplicate each runtime's option surface.
type ExecutorType string

const (
	ExecutorCommand   ExecutorType = "command"
	ExecutorDocker    ExecutorType = "docker"
	ExecutorPodman    ExecutorType = "podman"
	ExecutorApptainer ExecutorType = "apptainer"
)

// UnmarshalYAML rejects empty strings and non-scalar executor values while
// leaving an omitted value at zero so ApplyDefaults can select command.
func (executor *ExecutorType) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag == "!!null" {
		return errors.New("executor must be a non-empty scalar string")
	}
	if strings.TrimSpace(node.Value) == "" {
		return errors.New("executor must be a non-empty scalar string")
	}
	*executor = ExecutorType(node.Value)
	return nil
}

// Duration is a time.Duration that is encoded as a duration string in YAML.
type Duration struct {
	time.Duration
	set bool
}

// UnmarshalYAML parses values such as "250ms", "10s", and "15m".
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar string")
	}

	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	d.Duration = value
	d.set = true
	return nil
}

// MarshalYAML emits a duration string rather than an integer nanosecond count.
func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

// Config is the complete slipway configuration.
type Config struct {
	Queue    QueueConfig    `yaml:"queue"`
	Database DatabaseConfig `yaml:"database"`
	Watches  []WatchConfig  `yaml:"watches"`
}

// QueueConfig controls worker concurrency and retry behavior.
type QueueConfig struct {
	Workers    int      `yaml:"workers"`
	MaxRetries int      `yaml:"max_retries"`
	RetryDelay Duration `yaml:"retry_delay"`
}

// DatabaseConfig locates the authoritative SQLite queue.
type DatabaseConfig struct {
	Path string `yaml:"path"`
}

// WatchConfig describes one filesystem source and its command pipeline.
type WatchConfig struct {
	Name              string          `yaml:"name"`
	Path              string          `yaml:"path"`
	Recursive         bool            `yaml:"recursive"`
	ProcessExisting   bool            `yaml:"process_existing"`
	ReprocessOnChange bool            `yaml:"reprocess_on_change"`
	Include           []string        `yaml:"include"`
	Exclude           []string        `yaml:"exclude"`
	SettleFor         Duration        `yaml:"settle_for"`
	Pipeline          []CommandConfig `yaml:"pipeline"`
}

// MountConfig describes one bind mount for a structured container invocation.
type MountConfig struct {
	Source   string `yaml:"source"`
	Target   string `yaml:"target"`
	ReadOnly bool   `yaml:"read_only"`
}

// CommandConfig describes one process invocation. Program and Args are kept
// separate so callers never need to construct a shell command string. The
// container fields provide a structured alternative to raw runtime CLI args.
type CommandConfig struct {
	Name          string            `yaml:"name"`
	Executor      ExecutorType      `yaml:"executor"`
	Program       string            `yaml:"program"`
	Args          []string          `yaml:"args"`
	Image         string            `yaml:"image"`
	Mounts        []MountConfig     `yaml:"mounts"`
	ContainerEnv  map[string]string `yaml:"container_env"`
	ContainerArgs []string          `yaml:"container_args"`
	Command       string            `yaml:"command"`
	CommandArgs   []string          `yaml:"command_args"`
	Timeout       Duration          `yaml:"timeout"`
	WorkingDir    string            `yaml:"working_directory"`
	Output        string            `yaml:"output"`
	Env           map[string]string `yaml:"env"`

	structuredContainer bool
	containerCommandSet bool
}

// ExecutionArgs returns the actual host-process arguments for a pipeline step.
// Legacy container entries retain their raw Args. Structured entries are
// translated into a deterministic runtime CLI invocation, with every value
// kept as a separate process argument.
func (command CommandConfig) ExecutionArgs() []string {
	if !command.usesStructuredContainer() {
		return append([]string(nil), command.Args...)
	}

	operation := "run"
	if command.Executor == ExecutorApptainer && command.Command != "" {
		operation = "exec"
	}
	args := []string{operation}
	if command.Executor == ExecutorApptainer {
		// Apptainer evaluates container arguments and environment values by
		// default. Structured values are data, so keep them literal.
		args = append(args, "--no-eval")
	}
	args = append(args, command.ContainerArgs...)
	for _, mount := range command.Mounts {
		args = append(args, "--mount", formatBindMount(mount))
	}

	environmentKeys := make([]string, 0, len(command.ContainerEnv))
	for key := range command.ContainerEnv {
		environmentKeys = append(environmentKeys, key)
	}
	sort.Strings(environmentKeys)
	for _, key := range environmentKeys {
		environment := key + "=" + command.ContainerEnv[key]
		if command.Executor == ExecutorApptainer {
			// Apptainer's --env parser treats its value as a CSV record. Encode
			// the complete KEY=value assignment so commas and quotes in the value
			// cannot turn into additional environment entries.
			environment = formatApptainerEnvironment(environment)
		}
		args = append(args, "--env", environment)
	}
	args = append(args, command.Image)
	if command.Command != "" {
		args = append(args, command.Command)
	}
	args = append(args, command.CommandArgs...)
	return args
}

func formatApptainerEnvironment(environment string) string {
	fields := []string{environment}
	if strings.Count(environment, "=") == 1 && strings.ContainsRune(environment, '"') {
		// pflag's stringToString value skips CSV parsing when it sees exactly
		// one equals sign. Repeating the same assignment is harmless after it
		// is decoded into a map, and forces the CSV path that restores quotes.
		fields = append(fields, environment)
	}
	return formatCSVFields(fields)
}

func formatBindMount(mount MountConfig) string {
	fields := []string{"type=bind", "source=" + mount.Source, "target=" + mount.Target}
	if mount.ReadOnly {
		fields = append(fields, "ro")
	}
	return formatCSVFields(fields)
}

func formatCSVFields(fields []string) string {
	var encoded strings.Builder
	writer := csv.NewWriter(&encoded)
	_ = writer.Write(fields)
	writer.Flush()
	return strings.TrimSuffix(encoded.String(), "\n")
}

func (command CommandConfig) usesStructuredContainer() bool {
	return command.structuredContainer ||
		command.Image != "" ||
		len(command.Mounts) > 0 ||
		len(command.ContainerEnv) > 0 ||
		len(command.ContainerArgs) > 0 ||
		command.Command != "" ||
		len(command.CommandArgs) > 0
}

// ValidateExecution checks values that may have changed during per-job
// template expansion. Load performs the same checks on the configured values.
func (command CommandConfig) ValidateExecution() error {
	if !command.usesStructuredContainer() {
		return nil
	}
	return command.validateStructuredContainer()
}

func (command CommandConfig) validateStructuredContainer() error {
	if len(command.Args) > 0 {
		return errors.New("args cannot be combined with structured container fields; use container_args and command_args")
	}
	if strings.TrimSpace(command.Image) == "" {
		return errors.New("image is required for structured container execution")
	}
	if (command.containerCommandSet || command.Command != "") && strings.TrimSpace(command.Command) == "" {
		return errors.New("command must not be blank")
	}
	for index, mount := range command.Mounts {
		if strings.TrimSpace(mount.Source) == "" {
			return fmt.Errorf("mounts[%d].source is required", index)
		}
		if strings.TrimSpace(mount.Target) == "" {
			return fmt.Errorf("mounts[%d].target is required", index)
		}
		if !containerTargetCanBeAbsolute(mount.Target) {
			return fmt.Errorf("mounts[%d].target must be an absolute container path", index)
		}
	}
	for key := range command.ContainerEnv {
		if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
			return fmt.Errorf("container_env contains invalid variable name %q", key)
		}
	}
	for index, argument := range command.ContainerArgs {
		if argument == "--" {
			return fmt.Errorf("container_args[%d] must not be -- because generated mount and environment options follow container_args", index)
		}
	}
	return nil
}

func containerTargetCanBeAbsolute(target string) bool {
	return path.IsAbs(target) || strings.HasPrefix(target, fileTemplate) || strings.HasPrefix(target, dirTemplate)
}

// Load decodes, defaults, and validates a YAML configuration file. Unknown
// fields are rejected so configuration typos fail at startup.
func Load(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("decode config: multiple YAML documents are not supported")
		}
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("inspect config fields: %w", err)
	}
	if err := markContainerFieldPresence(&cfg, file); err != nil {
		return nil, fmt.Errorf("inspect config fields: %w", err)
	}

	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := resolveConfiguredPaths(&cfg, filename); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// markContainerFieldPresence distinguishes omitted structured fields from
// explicitly empty ones. That both makes empty declarations fail validation
// and preserves structured mode when a per-job template later expands to an
// empty string.
func markContainerFieldPresence(cfg *Config, source io.Reader) error {
	var fields struct {
		Watches []struct {
			Pipeline []map[string]any `yaml:"pipeline"`
		} `yaml:"watches"`
	}
	if err := yaml.NewDecoder(source).Decode(&fields); err != nil {
		return err
	}
	if len(fields.Watches) != len(cfg.Watches) {
		return errors.New("decoded watch count changed while inspecting fields")
	}
	structuredFields := [...]string{
		"image",
		"mounts",
		"container_env",
		"container_args",
		"command",
		"command_args",
	}
	for watchIndex := range cfg.Watches {
		if len(fields.Watches[watchIndex].Pipeline) != len(cfg.Watches[watchIndex].Pipeline) {
			return fmt.Errorf("decoded pipeline count changed for watches[%d] while inspecting fields", watchIndex)
		}
		for commandIndex := range cfg.Watches[watchIndex].Pipeline {
			command := &cfg.Watches[watchIndex].Pipeline[commandIndex]
			present := fields.Watches[watchIndex].Pipeline[commandIndex]
			for _, name := range structuredFields {
				if _, exists := present[name]; exists {
					command.structuredContainer = true
					break
				}
			}
			_, command.containerCommandSet = present["command"]
		}
	}
	return nil
}

// ApplyDefaults fills values whose zero value is not useful operationally.
// Explicit non-zero queue, duration, path, and executor values are preserved.
func (c *Config) ApplyDefaults() {
	if c == nil {
		return
	}
	if c.Queue.Workers == 0 {
		c.Queue.Workers = runtime.NumCPU()
	}
	if !c.Queue.RetryDelay.set {
		c.Queue.RetryDelay.Duration = defaultRetryDelay
	}
	if strings.TrimSpace(c.Database.Path) == "" {
		c.Database.Path = defaultDatabasePath
	}
	for i := range c.Watches {
		if !c.Watches[i].SettleFor.set {
			c.Watches[i].SettleFor.Duration = defaultSettleFor
		}
		for j := range c.Watches[i].Pipeline {
			command := &c.Watches[i].Pipeline[j]
			if command.Executor == "" {
				command.Executor = ExecutorCommand
			}
			if strings.TrimSpace(command.Program) == "" {
				command.Program = command.Executor.defaultProgram()
			}
		}
	}
}

// Validate checks semantic constraints that YAML decoding alone cannot express.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if c.Queue.Workers <= 0 {
		return errors.New("queue.workers must be greater than zero")
	}
	if c.Queue.MaxRetries < 0 {
		return errors.New("queue.max_retries must not be negative")
	}
	if c.Queue.RetryDelay.Duration < 0 {
		return errors.New("queue.retry_delay must not be negative")
	}
	if strings.TrimSpace(c.Database.Path) == "" {
		return errors.New("database.path is required")
	}
	if len(c.Watches) == 0 {
		return errors.New("at least one watch is required")
	}

	names := make(map[string]struct{}, len(c.Watches))
	for i := range c.Watches {
		watch := &c.Watches[i]
		prefix := fmt.Sprintf("watches[%d]", i)
		watch.Name = strings.TrimSpace(watch.Name)
		if watch.Name == "" {
			return fmt.Errorf("%s.name is required", prefix)
		}
		if _, exists := names[watch.Name]; exists {
			return fmt.Errorf("watch name %q is duplicated", watch.Name)
		}
		names[watch.Name] = struct{}{}
		if strings.TrimSpace(watch.Path) == "" {
			return fmt.Errorf("%s.path is required", prefix)
		}
		if watch.SettleFor.Duration < 0 {
			return fmt.Errorf("%s.settle_for must not be negative", prefix)
		}
		for _, group := range []struct {
			name     string
			patterns []string
		}{
			{name: "include", patterns: watch.Include},
			{name: "exclude", patterns: watch.Exclude},
		} {
			for j, pattern := range group.patterns {
				if err := validateGlob(pattern); err != nil {
					return fmt.Errorf("%s.%s[%d]: %w", prefix, group.name, j, err)
				}
			}
		}
		if len(watch.Pipeline) == 0 {
			return fmt.Errorf("%s.pipeline must contain at least one command", prefix)
		}
		for j := range watch.Pipeline {
			command := &watch.Pipeline[j]
			commandPrefix := fmt.Sprintf("%s.pipeline[%d]", prefix, j)
			command.Name = strings.TrimSpace(command.Name)
			if command.Name == "" {
				return fmt.Errorf("%s.name is required", commandPrefix)
			}
			if !command.Executor.valid() {
				return fmt.Errorf("%s.executor %q is invalid; must be one of command, docker, podman, or apptainer", commandPrefix, command.Executor)
			}
			if strings.TrimSpace(command.Program) == "" {
				return fmt.Errorf("%s.program is required", commandPrefix)
			}
			structuredContainer := command.usesStructuredContainer()
			command.structuredContainer = structuredContainer
			if command.Command != "" {
				command.containerCommandSet = true
			}
			if command.Executor == ExecutorCommand && structuredContainer {
				return fmt.Errorf("%s container fields require a docker, podman, or apptainer executor", commandPrefix)
			}
			if command.Executor != ExecutorCommand && structuredContainer {
				if err := command.validateStructuredContainer(); err != nil {
					return fmt.Errorf("%s.%w", commandPrefix, err)
				}
			}
			if command.Timeout.Duration < 0 {
				return fmt.Errorf("%s.timeout must not be negative", commandPrefix)
			}
			for key := range command.Env {
				if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
					return fmt.Errorf("%s.env contains invalid variable name %q", commandPrefix, key)
				}
			}
		}
	}
	return nil
}

func (executor ExecutorType) valid() bool {
	switch executor {
	case ExecutorCommand, ExecutorDocker, ExecutorPodman, ExecutorApptainer:
		return true
	default:
		return false
	}
}

func (executor ExecutorType) defaultProgram() string {
	switch executor {
	case ExecutorDocker, ExecutorPodman, ExecutorApptainer:
		return string(executor)
	default:
		return ""
	}
}

func validateGlob(pattern string) error {
	if pattern == "" {
		return errors.New("glob must not be empty")
	}
	pattern = strings.TrimPrefix(strings.ReplaceAll(pattern, "\\", "/"), "./")
	for _, segment := range strings.Split(pattern, "/") {
		if segment == "**" {
			continue
		}
		if _, err := path.Match(segment, "glob-check"); err != nil {
			return fmt.Errorf("invalid glob %q: %w", pattern, err)
		}
	}
	return nil
}
