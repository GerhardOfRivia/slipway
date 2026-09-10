package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExpanderString(t *testing.T) {
	t.Parallel()
	file := filepath.Join(string(filepath.Separator), "tmp", "a directory", "report.final.csv")
	expander := NewExpander(TemplateData{File: file, JobID: 42})
	for _, test := range []struct{ input, want string }{
		{"{{file}}", file},
		{"{{dir}}", filepath.Dir(file)},
		{"{{basename}}", "report.final.csv"},
		{"{{stem}}", "report.final"},
		{"{{ext}}", ".csv"},
		{"job={{job_id}}", "job=42"},
		{"{{basename}}:{{job_id}}", "report.final.csv:42"},
		{"{{unknown}}", "{{unknown}}"},
	} {
		if got := expander.String(test.input); got != test.want {
			t.Errorf("String(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestLocalSavesStdoutRelativeToWorkingDirectory(t *testing.T) {
	t.Parallel()

	workingDirectory := t.TempDir()
	outputName := "saved output;literal.txt"
	outputPath := filepath.Join(workingDirectory, outputName)
	if err := os.WriteFile(outputPath, []byte("stale contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	result, err := NewLocal(nil).Execute(context.Background(), Command{
		Name:       "save streams",
		Program:    executable,
		Args:       []string{"-test.run=^TestExecutorHelperProcess$", "--"},
		WorkingDir: workingDirectory,
		Output:     outputName,
		Env:        map[string]string{"SLIPWAY_EXECUTOR_HELPER": "streams"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v; stderr = %s", err, result.Stderr)
	}
	if result.Stdout != "standard output\n" {
		t.Fatalf("stdout = %q", result.Stdout)
	}
	if result.Stderr != "standard error\n" {
		t.Fatalf("stderr = %q", result.Stderr)
	}
	saved, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(saved), result.Stdout; got != want {
		t.Fatalf("saved output = %q, want %q", got, want)
	}
}

func TestLocalOutputOpenFailureDoesNotStartCommand(t *testing.T) {
	t.Parallel()

	marker := filepath.Join(t.TempDir(), "started")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewLocal(nil).Execute(context.Background(), Command{
		Name:    "cannot save",
		Program: executable,
		Args:    []string{"-test.run=^TestExecutorHelperProcess$", "--"},
		Output:  filepath.Join(t.TempDir(), "missing", "output.txt"),
		Env: map[string]string{
			"SLIPWAY_EXECUTOR_HELPER": "mark-started",
			"SLIPWAY_MARKER":          marker,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "open output") {
		t.Fatalf("Execute() error = %v, want output-open error", err)
	}
	if result.ExitCode != -1 || !result.StartedAt.IsZero() {
		t.Fatalf("result indicates command started: %+v", result)
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("command ran despite output-open failure; marker stat error = %v", statErr)
	}
}

func TestLocalPassesUnsafeLookingPathAsOneLiteralArgument(t *testing.T) {
	t.Parallel()

	marker := filepath.Join(t.TempDir(), "must-not-be-created")
	file := "some folder/a file;$(touch " + marker + ")&.csv"
	command := Command{
		Name:    "echo arguments",
		Program: os.Args[0],
		Args:    []string{"-test.run=^TestExecutorHelperProcess$", "--", file, "literal * ? | >"},
		Env:     map[string]string{"SLIPWAY_EXECUTOR_HELPER": "echo"},
	}

	result, err := NewLocal(nil).Execute(context.Background(), command)
	if err != nil {
		t.Fatalf("Execute() error = %v; stderr = %s", err, result.Stderr)
	}
	var got []string
	if err := json.Unmarshal([]byte(result.Stdout), &got); err != nil {
		t.Fatalf("decode helper output %q: %v", result.Stdout, err)
	}
	want := []string{file, "literal * ? | >"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("helper arguments = %#v, want %#v", got, want)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shell-like argument was evaluated; marker stat error = %v", err)
	}
}

func TestLocalRunsExplicitShellWithExpansionAndLiteralPositionalArgs(t *testing.T) {
	t.Parallel()

	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh is not available")
	}
	workingDirectory := t.TempDir()
	for _, name := range []string{"alpha.txt", "two words.txt", "ignored.csv"} {
		if err := os.WriteFile(filepath.Join(workingDirectory, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	marker := filepath.Join(workingDirectory, "must-not-be-created")
	unsafeArgument := "$(touch " + marker + "); literal *"

	result, err := NewLocal(nil).Execute(context.Background(), Command{
		Name:       "shell expansion",
		Program:    shell,
		WorkingDir: workingDirectory,
		Env:        map[string]string{"MODE": "batch mode"},
		Args: []string{
			"-c",
			`printf 'mode=%s\n' "$MODE"; printf 'arg=%s\n' "$1"; for path in ./*.txt; do printf 'file=%s\n' "$path"; done`,
			"slipway-shell",
			unsafeArgument,
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v; stderr = %s", err, result.Stderr)
	}
	want := "mode=batch mode\narg=" + unsafeArgument + "\nfile=./alpha.txt\nfile=./two words.txt\n"
	if result.Stdout != want {
		t.Fatalf("stdout = %q, want %q", result.Stdout, want)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("positional argument was evaluated as shell source; marker stat error = %v", err)
	}
}

func TestLocalTimeoutCapturesPartialOutput(t *testing.T) {
	t.Parallel()

	result, err := NewLocal(nil).Execute(context.Background(), Command{
		Name:    "slow command",
		Program: os.Args[0],
		Args:    []string{"-test.run=^TestExecutorHelperProcess$", "--"},
		Env:     map[string]string{"SLIPWAY_EXECUTOR_HELPER": "sleep"},
		Timeout: 75 * time.Millisecond,
	})
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute() error = %v, want timeout", err)
	}
	if !result.TimedOut {
		t.Fatal("result did not record timeout")
	}
	if result.Stdout != "started\n" {
		t.Fatalf("stdout = %q, want partial output", result.Stdout)
	}
	if result.ExitCode == 0 {
		t.Fatalf("exit code = %d, want nonzero", result.ExitCode)
	}
}

func TestLocalBoundsCapturedOutput(t *testing.T) {
	t.Parallel()

	outputPath := filepath.Join(t.TempDir(), "complete-output.txt")
	result, err := NewLocal(nil).Execute(context.Background(), Command{
		Name:    "verbose command",
		Program: os.Args[0],
		Args:    []string{"-test.run=^TestExecutorHelperProcess$", "--"},
		Output:  outputPath,
		Env:     map[string]string{"SLIPWAY_EXECUTOR_HELPER": "large-output"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	marker := fmt.Sprintf("\n[slipway: output truncated after %d bytes]\n", maxCapturedOutputBytes)
	for name, output := range map[string]string{"stdout": result.Stdout, "stderr": result.Stderr} {
		if got, want := len(output), maxCapturedOutputBytes+len(marker); got != want {
			t.Errorf("%s length = %d, want %d", name, got, want)
		}
		if !strings.HasSuffix(output, marker) {
			t.Errorf("%s does not end with truncation marker", name)
		}
	}
	saved, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	chunkSize := 64 << 10
	wantSavedBytes := (maxCapturedOutputBytes/chunkSize + 1) * chunkSize
	if len(saved) != wantSavedBytes {
		t.Fatalf("saved output length = %d, want complete %d-byte stream", len(saved), wantSavedBytes)
	}
	if strings.Contains(string(saved), "[slipway: output truncated") {
		t.Fatal("saved output contains the history truncation marker")
	}
}

func TestExecutorHelperProcess(t *testing.T) {
	mode := os.Getenv("SLIPWAY_EXECUTOR_HELPER")
	if mode == "" {
		return
	}
	separator := 0
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i + 1
			break
		}
	}

	switch mode {
	case "echo":
		if err := json.NewEncoder(os.Stdout).Encode(os.Args[separator:]); err != nil {
			panic(err)
		}
	case "sleep":
		fmt.Println("started")
		time.Sleep(10 * time.Second)
	case "large-output":
		chunk := strings.Repeat("x", 64<<10)
		for written := 0; written <= maxCapturedOutputBytes; written += len(chunk) {
			if _, err := fmt.Fprint(os.Stdout, chunk); err != nil {
				panic(err)
			}
			if _, err := fmt.Fprint(os.Stderr, chunk); err != nil {
				panic(err)
			}
		}
	case "streams":
		fmt.Fprintln(os.Stdout, "standard output")
		fmt.Fprintln(os.Stderr, "standard error")
	case "mark-started":
		if err := os.WriteFile(os.Getenv("SLIPWAY_MARKER"), []byte("started"), 0o600); err != nil {
			panic(err)
		}
	default:
		panic("unknown helper mode: " + mode)
	}
	os.Exit(0)
}
