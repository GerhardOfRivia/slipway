package config

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEffectiveFingerprintIsStable(t *testing.T) {
	t.Parallel()

	cfg := fingerprintTestConfig(
		map[string]string{"MODE": "batch", "REGION": "west"},
		map[string]string{"FORMAT": "csv", "STRICT": "true"},
	)
	first, err := EffectiveFingerprint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EffectiveFingerprint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("fingerprints differ for the same config: %q != %q", first, second)
	}
	if len(first) != sha256HexLength {
		t.Fatalf("fingerprint length = %d, want %d", len(first), sha256HexLength)
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Fatalf("fingerprint %q is not lowercase hexadecimal: %v", first, err)
	}
	if first != strings.ToLower(first) {
		t.Fatalf("fingerprint %q is not lowercase", first)
	}
}

func TestEffectiveFingerprintIgnoresMapInsertionOrder(t *testing.T) {
	t.Parallel()

	first := fingerprintTestConfig(
		map[string]string{"MODE": "batch", "REGION": "west"},
		map[string]string{"FORMAT": "csv", "STRICT": "true"},
	)
	second := fingerprintTestConfig(
		map[string]string{"REGION": "west", "MODE": "batch"},
		map[string]string{"STRICT": "true", "FORMAT": "csv"},
	)

	firstFingerprint, err := EffectiveFingerprint(first)
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, err := EffectiveFingerprint(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint != secondFingerprint {
		t.Fatalf("map insertion order changed fingerprint: %q != %q", firstFingerprint, secondFingerprint)
	}
}

func TestEffectiveFingerprintMatchesEquivalentLoadedYAML(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	minimalPath := filepath.Join(directory, "minimal.yaml")
	explicitPath := filepath.Join(directory, "explicit.yaml")
	minimal := `
queue:
  workers: 2
database:
  path: ./queue.db
watches:
  - name: incoming
    path: ./incoming
    pipeline:
      - name: inspect
        program: /bin/true
`
	explicit := `
# Key ordering, comments, and explicit defaults do not change behavior.
watches:
  - pipeline:
      - executor: command
        program: /bin/true
        name: inspect
    settle_for: 1s
    path: ./incoming
    name: incoming
database: {path: ./queue.db}
queue:
  retry_delay: 10s
  max_retries: 0
  workers: 2
`
	if err := os.WriteFile(minimalPath, []byte(minimal), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(explicitPath, []byte(explicit), 0o600); err != nil {
		t.Fatal(err)
	}

	minimalConfig, err := Load(minimalPath)
	if err != nil {
		t.Fatal(err)
	}
	explicitConfig, err := Load(explicitPath)
	if err != nil {
		t.Fatal(err)
	}
	minimalFingerprint, err := EffectiveFingerprint(minimalConfig)
	if err != nil {
		t.Fatal(err)
	}
	explicitFingerprint, err := EffectiveFingerprint(explicitConfig)
	if err != nil {
		t.Fatal(err)
	}
	if minimalFingerprint != explicitFingerprint {
		t.Fatalf("equivalent loaded YAML fingerprints differ: %q != %q", minimalFingerprint, explicitFingerprint)
	}
}

func TestEffectiveFingerprintChangesWithMaterialConfigValues(t *testing.T) {
	t.Parallel()

	baseline := fingerprintTestConfig(
		map[string]string{"MODE": "batch", "REGION": "west"},
		map[string]string{"FORMAT": "csv", "STRICT": "true"},
	)
	changed := fingerprintTestConfig(
		map[string]string{"MODE": "stream", "REGION": "west"},
		map[string]string{"FORMAT": "csv", "STRICT": "true"},
	)

	baselineFingerprint, err := EffectiveFingerprint(baseline)
	if err != nil {
		t.Fatal(err)
	}
	changedFingerprint, err := EffectiveFingerprint(changed)
	if err != nil {
		t.Fatal(err)
	}
	if baselineFingerprint == changedFingerprint {
		t.Fatalf("material config change retained fingerprint %q", baselineFingerprint)
	}
}

func TestEffectiveFingerprintRejectsNilConfig(t *testing.T) {
	t.Parallel()

	if fingerprint, err := EffectiveFingerprint(nil); err == nil {
		t.Fatalf("EffectiveFingerprint(nil) = %q, nil; want an error", fingerprint)
	}
}

const sha256HexLength = 64

func fingerprintTestConfig(environment, containerEnvironment map[string]string) *Config {
	return &Config{
		Queue: QueueConfig{
			Workers:    2,
			MaxRetries: 3,
			RetryDelay: Duration{Duration: 10 * time.Second},
		},
		Database: DatabaseConfig{Path: "/var/lib/slipway/queue.db"},
		Watches: []WatchConfig{{
			Name:              "incoming",
			Path:              "/srv/incoming",
			Recursive:         true,
			ProcessExisting:   true,
			ReprocessOnChange: true,
			Include:           []string{"*.csv"},
			Exclude:           []string{"*.partial"},
			SettleFor:         Duration{Duration: time.Second},
			Pipeline: []CommandConfig{{
				Name:          "process",
				Executor:      ExecutorDocker,
				Program:       "docker",
				Image:         "example/processor:1.2.3",
				ContainerArgs: []string{"--rm"},
				ContainerEnv:  containerEnvironment,
				Command:       "/app/process",
				CommandArgs:   []string{"--input", "/data/input.csv"},
				Timeout:       Duration{Duration: time.Minute},
				WorkingDir:    "/var/tmp/slipway",
				Output:        "result.json",
				Env:           environment,
			}},
		}},
	}
}
