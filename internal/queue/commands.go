package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// StartCommand records command metadata before the caller starts the process.
func (s *Store) StartCommand(ctx context.Context, command CommandStart) (int64, error) {
	if command.RunID <= 0 {
		return 0, errors.New("queue: invalid run id")
	}
	if command.Sequence < 0 {
		return 0, errors.New("queue: command sequence cannot be negative")
	}
	if strings.TrimSpace(command.Program) == "" {
		return 0, errors.New("queue: command program is empty")
	}
	if command.Timeout < 0 {
		return 0, errors.New("queue: command timeout cannot be negative")
	}

	argsJSON, err := marshalStrings(command.Args)
	if err != nil {
		return 0, fmt.Errorf("queue: encode command arguments: %w", err)
	}
	envJSON, err := marshalStrings(command.Env)
	if err != nil {
		return 0, fmt.Errorf("queue: encode command environment: %w", err)
	}
	now := s.timestamp()
	result, err := s.db.ExecContext(ctx, `
INSERT INTO command_executions (
    run_id, sequence, name, program, args_json, env_json, working_dir,
    timeout_ns, status, started_at
)
SELECT id, ?, ?, ?, ?, ?, ?, ?, ?, ?
FROM runs
WHERE id = ? AND status = ?`,
		command.Sequence,
		command.Name,
		command.Program,
		argsJSON,
		envJSON,
		command.WorkingDir,
		int64(command.Timeout),
		CommandRunning,
		unixNano(now),
		command.RunID,
		StatusRunning,
	)
	if err != nil {
		return 0, fmt.Errorf("queue: start command: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return 0, fmt.Errorf("queue: start command: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("queue: get command id: %w", err)
	}
	return id, nil
}

// CompleteCommand records the result of a running command.
func (s *Store) CompleteCommand(ctx context.Context, commandID int64, result CommandResult) error {
	if commandID <= 0 {
		return errors.New("queue: invalid command id")
	}
	if result.Status != CommandSucceeded && result.Status != CommandFailed {
		return fmt.Errorf("queue: invalid terminal command status %q", result.Status)
	}
	now := s.timestamp()
	dbResult, err := s.db.ExecContext(ctx, `
UPDATE command_executions
SET status = ?, exit_code = ?, stdout = ?, stderr = ?, error = ?, finished_at = ?
WHERE id = ? AND status = ?`,
		result.Status,
		result.ExitCode,
		result.Stdout,
		result.Stderr,
		result.Error,
		unixNano(now),
		commandID,
		CommandRunning,
	)
	if err != nil {
		return fmt.Errorf("queue: complete command: %w", err)
	}
	if err := requireOneRow(dbResult); err != nil {
		return fmt.Errorf("queue: complete command: %w", err)
	}
	return nil
}

func marshalStrings(values []string) (string, error) {
	if values == nil {
		values = []string{}
	}
	encoded, err := json.Marshal(values)
	return string(encoded), err
}
