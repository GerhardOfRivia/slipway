package webui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/control"
	"github.com/GerhardOfRivia/slipway/internal/queue"
)

const (
	defaultJobLimit         = 50
	maximumJobLimit         = 200
	maxMutationBody         = 1024
	queueOverviewTimeout    = 10 * time.Second
	queueReadTimeout        = 3 * time.Second
	queueOverviewWorkers    = 4
	controlOperationTimeout = 25 * time.Second
)

var errQueueUninitialized = errors.New("queue database has not been created")

type apiServer struct {
	manager *control.Manager
	logger  *slog.Logger
}

type queueCounts struct {
	Queued    int64 `json:"queued"`
	Running   int64 `json:"running"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Total     int64 `json:"total"`
}

type queueSummary struct {
	ID             string            `json:"id"`
	DisplayName    string            `json:"display_name"`
	ConfigPath     string            `json:"config_path"`
	ConfigHash     string            `json:"config_hash"`
	DatabasePath   string            `json:"database_path"`
	Watches        []string          `json:"watches"`
	DatabaseState  string            `json:"database_state"`
	Counts         queueCounts       `json:"counts"`
	ActiveInstance *control.Instance `json:"active_instance,omitempty"`
	Error          string            `json:"error,omitempty"`
}

type queuesResponse struct {
	Queues      []queueSummary `json:"queues"`
	GeneratedAt time.Time      `json:"generated_at"`
}

type instancesResponse struct {
	Instances   []control.Instance `json:"instances"`
	GeneratedAt time.Time          `json:"generated_at"`
}

type instanceResponse struct {
	Instance control.Instance `json:"instance"`
}

type jobView struct {
	ID          int64        `json:"id"`
	WatchName   string       `json:"watch_name"`
	Path        string       `json:"path"`
	Fingerprint string       `json:"fingerprint,omitempty"`
	Status      queue.Status `json:"status"`
	Attempts    int          `json:"attempts"`
	MaxRetries  int          `json:"max_retries"`
	AvailableAt time.Time    `json:"available_at"`
	LastError   string       `json:"last_error,omitempty"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	StartedAt   *time.Time   `json:"started_at,omitempty"`
	FinishedAt  *time.Time   `json:"finished_at,omitempty"`
}

type jobsResponse struct {
	Jobs        []jobView `json:"jobs"`
	Limit       int       `json:"limit"`
	Offset      int       `json:"offset"`
	HasMore     bool      `json:"has_more"`
	GeneratedAt time.Time `json:"generated_at"`
}

type runView struct {
	ID         int64         `json:"id"`
	Attempt    int           `json:"attempt"`
	Status     queue.Status  `json:"status"`
	Error      string        `json:"error,omitempty"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
	Commands   []commandView `json:"commands"`
}

type commandView struct {
	ID          int64               `json:"id"`
	Sequence    int                 `json:"sequence"`
	Name        string              `json:"name"`
	Program     string              `json:"program"`
	Args        []string            `json:"args"`
	WorkingDir  string              `json:"working_directory,omitempty"`
	Timeout     string              `json:"timeout"`
	Status      queue.CommandStatus `json:"status"`
	ExitCode    *int                `json:"exit_code,omitempty"`
	Error       string              `json:"error,omitempty"`
	StartedAt   time.Time           `json:"started_at"`
	FinishedAt  *time.Time          `json:"finished_at,omitempty"`
	StdoutBytes int64               `json:"stdout_bytes"`
	StderrBytes int64               `json:"stderr_bytes"`
}

type jobResponse struct {
	Job  jobView   `json:"job"`
	Runs []runView `json:"runs"`
}

type commandOutputResponse struct {
	CommandID int64  `json:"command_id"`
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
}

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (api *apiServer) handleQueues(output http.ResponseWriter, request *http.Request) {
	known := api.manager.KnownQueues()
	active := api.manager.List(false)
	activeByDatabase := make(map[string]control.Instance, len(active))
	for _, instance := range active {
		activeByDatabase[instance.DatabasePath] = instance
	}

	result := queuesResponse{
		Queues:      make([]queueSummary, len(known)),
		GeneratedAt: time.Now().UTC(),
	}
	for index, item := range known {
		result.Queues[index] = queueSummary{
			ID:            queueID(item.Identity),
			DisplayName:   displayName(item.ConfigPath),
			ConfigPath:    item.ConfigPath,
			ConfigHash:    item.ConfigHash,
			DatabasePath:  item.DatabasePath,
			Watches:       append([]string(nil), item.WatchNames...),
			DatabaseState: "unavailable",
			Error:         "Queue summary was not read before the dashboard deadline",
		}
		for _, databasePath := range knownQueuePaths(item) {
			if instance, exists := activeByDatabase[databasePath]; exists {
				copy := instance
				result.Queues[index].ActiveInstance = &copy
				break
			}
		}
	}

	ctx, cancel := context.WithTimeout(request.Context(), queueOverviewTimeout)
	defer cancel()
	jobs := make(chan int)
	workers := min(queueOverviewWorkers, len(known))
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				queueContext, cancelQueue := context.WithTimeout(ctx, queueReadTimeout)
				populateQueueSummary(queueContext, &result.Queues[index], known[index])
				cancelQueue()
			}
		}()
	}
	go func() {
		defer close(jobs)
		for index := range known {
			select {
			case jobs <- index:
			case <-ctx.Done():
				return
			}
		}
	}()
	wait.Wait()
	if request.Context().Err() != nil {
		return
	}
	writeJSON(output, http.StatusOK, result)
}

func populateQueueSummary(ctx context.Context, summary *queueSummary, known control.KnownQueue) {
	store, err := openKnownQueue(ctx, known)
	if errors.Is(err, errQueueUninitialized) {
		summary.DatabaseState = "missing"
		summary.Error = "Queue has not been created yet"
		return
	}
	if err != nil {
		summary.DatabaseState = "unavailable"
		if errors.Is(err, context.DeadlineExceeded) {
			summary.Error = "Queue summary timed out"
		} else {
			summary.Error = err.Error()
		}
		return
	}
	counts, countErr := store.Counts(ctx)
	closeErr := store.Close()
	if countErr != nil || closeErr != nil {
		summary.DatabaseState = "unavailable"
		summary.Error = errors.Join(countErr, closeErr).Error()
		return
	}
	summary.DatabaseState = "ready"
	summary.Error = ""
	summary.Counts = toCounts(counts)
}

func (api *apiServer) handleInstances(output http.ResponseWriter, request *http.Request) {
	all := true
	if value := request.URL.Query().Get("all"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			writeAPIError(output, http.StatusBadRequest, "invalid_request", fmt.Sprintf("invalid all value %q", value))
			return
		}
		all = parsed
	}
	instances := api.manager.List(all)
	if instances == nil {
		instances = []control.Instance{}
	}
	writeJSON(output, http.StatusOK, instancesResponse{Instances: instances, GeneratedAt: time.Now().UTC()})
}

func (api *apiServer) handleJobs(output http.ResponseWriter, request *http.Request) {
	known, ok := api.resolveQueue(request.PathValue("queueID"))
	if !ok {
		writeAPIError(output, http.StatusNotFound, "queue_not_found", "Queue not found")
		return
	}
	filter, limit, offset, err := jobFilter(request.URL.Query())
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	store, err := openKnownQueue(request.Context(), known)
	if err != nil {
		writeQueueUnavailable(output, err)
		return
	}
	defer store.Close()

	filter.Limit = limit + 1
	filter.Offset = offset
	jobs, err := store.ListJobs(request.Context(), filter)
	if err != nil {
		writeAPIError(output, http.StatusServiceUnavailable, "queue_unavailable", err.Error())
		return
	}
	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit]
	}
	views := make([]jobView, 0, len(jobs))
	for _, job := range jobs {
		views = append(views, toJobView(job, false))
	}
	writeJSON(output, http.StatusOK, jobsResponse{
		Jobs: views, Limit: limit, Offset: offset, HasMore: hasMore, GeneratedAt: time.Now().UTC(),
	})
}

func (api *apiServer) handleJob(output http.ResponseWriter, request *http.Request) {
	known, ok := api.resolveQueue(request.PathValue("queueID"))
	if !ok {
		writeAPIError(output, http.StatusNotFound, "queue_not_found", "Queue not found")
		return
	}
	jobID, err := positiveID(request.PathValue("jobID"), "job")
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	store, err := openKnownQueue(request.Context(), known)
	if err != nil {
		writeQueueUnavailable(output, err)
		return
	}
	defer store.Close()

	job, err := store.GetJob(request.Context(), jobID)
	if errors.Is(err, queue.ErrNotFound) {
		writeAPIError(output, http.StatusNotFound, "job_not_found", "Job not found")
		return
	}
	if err != nil {
		writeAPIError(output, http.StatusServiceUnavailable, "queue_unavailable", err.Error())
		return
	}
	runs, err := store.ListRuns(request.Context(), jobID)
	if err != nil {
		writeAPIError(output, http.StatusServiceUnavailable, "queue_unavailable", err.Error())
		return
	}
	runViews := make([]runView, 0, len(runs))
	for _, run := range runs {
		commands, commandErr := store.ListCommandSummaries(request.Context(), run.ID)
		if commandErr != nil {
			writeAPIError(output, http.StatusServiceUnavailable, "queue_unavailable", commandErr.Error())
			return
		}
		commandViews := make([]commandView, 0, len(commands))
		for _, command := range commands {
			commandViews = append(commandViews, toCommandView(command))
		}
		runViews = append(runViews, runView{
			ID: run.ID, Attempt: run.Attempt, Status: run.Status, Error: run.Error,
			StartedAt: run.StartedAt, FinishedAt: run.FinishedAt, Commands: commandViews,
		})
	}
	writeJSON(output, http.StatusOK, jobResponse{Job: toJobView(*job, true), Runs: runViews})
}

func (api *apiServer) handleCommandOutput(output http.ResponseWriter, request *http.Request) {
	known, ok := api.resolveQueue(request.PathValue("queueID"))
	if !ok {
		writeAPIError(output, http.StatusNotFound, "queue_not_found", "Queue not found")
		return
	}
	commandID, err := positiveID(request.PathValue("commandID"), "command")
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	store, err := openKnownQueue(request.Context(), known)
	if err != nil {
		writeQueueUnavailable(output, err)
		return
	}
	defer store.Close()
	history, err := store.GetCommandOutput(request.Context(), commandID)
	if errors.Is(err, queue.ErrNotFound) {
		writeAPIError(output, http.StatusNotFound, "command_not_found", "Command not found")
		return
	}
	if err != nil {
		writeAPIError(output, http.StatusServiceUnavailable, "queue_unavailable", err.Error())
		return
	}
	writeJSON(output, http.StatusOK, commandOutputResponse{
		CommandID: history.ID, Stdout: history.Stdout, Stderr: history.Stderr,
	})
}

func (api *apiServer) handleStart(output http.ResponseWriter, request *http.Request) {
	if err := validateMutation(output, request); err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	known, ok := api.resolveQueue(request.PathValue("queueID"))
	if !ok {
		writeAPIError(output, http.StatusNotFound, "queue_not_found", "Queue not found")
		return
	}
	operationContext, cancel := context.WithTimeout(request.Context(), controlOperationTimeout)
	defer cancel()
	instances, err := api.manager.StartKnownQueueContext(operationContext, known)
	if err != nil {
		writeManagerError(output, err, true)
		return
	}
	if len(instances) != 1 {
		writeAPIError(output, http.StatusInternalServerError, "internal_error", "Start did not return exactly one instance")
		return
	}
	writeJSON(output, http.StatusCreated, instanceResponse{Instance: instances[0]})
}

func (api *apiServer) handleStop(output http.ResponseWriter, request *http.Request) {
	if err := validateMutation(output, request); err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	instanceID := request.PathValue("instanceID")
	if !validInstanceID(instanceID) {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", "A full 12-character instance ID is required")
		return
	}
	operationContext, cancel := context.WithTimeout(request.Context(), controlOperationTimeout)
	defer cancel()
	instance, err := api.manager.Stop(operationContext, instanceID)
	if err != nil {
		writeManagerError(output, err, false)
		return
	}
	writeJSON(output, http.StatusOK, instanceResponse{Instance: instance})
}

func (api *apiServer) resolveQueue(id string) (control.KnownQueue, bool) {
	for _, known := range api.manager.KnownQueues() {
		if queueID(known.Identity) == id {
			return known, true
		}
	}
	return control.KnownQueue{}, false
}

func queueID(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

func displayName(configPath string) string {
	name := filepath.Base(configPath)
	return strings.TrimSuffix(name, filepath.Ext(name))
}

func openKnownQueue(ctx context.Context, known control.KnownQueue) (*queue.Store, error) {
	for _, databasePath := range knownQueuePaths(known) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := os.Stat(databasePath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect queue database: %w", err)
		}
		if !info.Mode().IsRegular() {
			return nil, errors.New("queue database is not a regular file")
		}
		store, err := queue.OpenReadOnlyContext(ctx, databasePath)
		if err != nil {
			return nil, err
		}
		return store, nil
	}
	return nil, errQueueUninitialized
}

func knownQueuePaths(known control.KnownQueue) []string {
	paths := make([]string, 0, 2+len(known.DatabaseAliases))
	for _, candidate := range append([]string{known.DatabasePath, known.Identity}, known.DatabaseAliases...) {
		if strings.TrimSpace(candidate) != "" && !slices.Contains(paths, candidate) {
			paths = append(paths, candidate)
		}
	}
	return paths
}

func jobFilter(values url.Values) (queue.JobFilter, int, int, error) {
	var filter queue.JobFilter
	if statusText := strings.TrimSpace(values.Get("status")); statusText != "" {
		status := queue.Status(strings.ToUpper(statusText))
		switch status {
		case queue.StatusQueued, queue.StatusRunning, queue.StatusSucceeded, queue.StatusFailed:
			filter.Status = status
		default:
			return filter, 0, 0, fmt.Errorf("unknown job status %q", statusText)
		}
	}
	filter.WatchName = strings.TrimSpace(values.Get("watch"))
	if len(filter.WatchName) > 256 {
		return filter, 0, 0, errors.New("watch filter is too long")
	}
	limit, err := queryInteger(values, "limit", defaultJobLimit)
	if err != nil || limit <= 0 || limit > maximumJobLimit {
		return filter, 0, 0, fmt.Errorf("limit must be between 1 and %d", maximumJobLimit)
	}
	offset, err := queryInteger(values, "offset", 0)
	if err != nil || offset < 0 {
		return filter, 0, 0, errors.New("offset must be a non-negative integer")
	}
	return filter, limit, offset, nil
}

func queryInteger(values url.Values, name string, fallback int) (int, error) {
	value := values.Get(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	return int(parsed), err
}

func positiveID(value, kind string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid %s ID %q", kind, value)
	}
	return id, nil
}

func validInstanceID(value string) bool {
	if len(value) != 12 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validateMutation(output http.ResponseWriter, request *http.Request) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errors.New("Content-Type must be application/json")
	}
	if request.Header.Get("X-slipway-Web") != "1" {
		return errors.New("X-slipway-Web header is required")
	}
	if site := request.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return errors.New("cross-site requests are not allowed")
	}
	if origin := request.Header.Get("Origin"); origin != "" {
		parsed, parseErr := url.Parse(origin)
		if parseErr != nil || parsed.Scheme != "http" || !strings.EqualFold(parsed.Host, request.Host) {
			return errors.New("request origin does not match the slipway web origin")
		}
	}

	request.Body = http.MaxBytesReader(output, request.Body, maxMutationBody)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var body struct{}
	if err := decoder.Decode(&body); err != nil {
		return fmt.Errorf("decode JSON request: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request contains more than one JSON value")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}

func toCounts(counts queue.QueueCounts) queueCounts {
	return queueCounts{
		Queued: counts.Queued, Running: counts.Running, Succeeded: counts.Succeeded,
		Failed: counts.Failed, Total: counts.Total,
	}
}

func toJobView(job queue.Job, includeFingerprint bool) jobView {
	view := jobView{
		ID: job.ID, WatchName: job.WatchName, Path: job.Path, Status: job.Status,
		Attempts: job.Attempts, MaxRetries: job.MaxRetries, AvailableAt: job.AvailableAt,
		LastError: job.LastError, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt,
		StartedAt: job.StartedAt, FinishedAt: job.FinishedAt,
	}
	if includeFingerprint {
		view.Fingerprint = job.Fingerprint
	}
	return view
}

func toCommandView(command queue.CommandSummary) commandView {
	return commandView{
		ID: command.ID, Sequence: command.Sequence, Name: command.Name,
		Program: command.Program, Args: append([]string(nil), command.Args...),
		WorkingDir: command.WorkingDir, Timeout: command.Timeout.String(),
		Status: command.Status, ExitCode: command.ExitCode, Error: command.Error,
		StartedAt: command.StartedAt, FinishedAt: command.FinishedAt,
		StdoutBytes: command.StdoutBytes, StderrBytes: command.StderrBytes,
	}
}

func writeQueueUnavailable(output http.ResponseWriter, err error) {
	if errors.Is(err, errQueueUninitialized) {
		writeAPIError(output, http.StatusConflict, "queue_uninitialized", "Queue has not been created yet")
		return
	}
	writeAPIError(output, http.StatusServiceUnavailable, "queue_unavailable", err.Error())
}

func writeManagerError(output http.ResponseWriter, err error, start bool) {
	switch {
	case errors.Is(err, control.ErrNotFound):
		writeAPIError(output, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, control.ErrAmbiguous):
		writeAPIError(output, http.StatusConflict, "ambiguous_selector", err.Error())
	case errors.Is(err, control.ErrAlreadyActive):
		writeAPIError(output, http.StatusConflict, "already_active", err.Error())
	case errors.Is(err, control.ErrNameInUse):
		writeAPIError(output, http.StatusConflict, "name_in_use", err.Error())
	case errors.Is(err, control.ErrNotActive):
		writeAPIError(output, http.StatusConflict, "not_active", err.Error())
	case errors.Is(err, control.ErrShuttingDown):
		writeAPIError(output, http.StatusServiceUnavailable, "shutting_down", err.Error())
	case errors.Is(err, control.ErrQueueChanged):
		writeAPIError(output, http.StatusConflict, "queue_changed", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeAPIError(output, http.StatusGatewayTimeout, "operation_timeout", "The control operation timed out")
	case start:
		writeAPIError(output, http.StatusUnprocessableEntity, "start_failed", err.Error())
	default:
		writeAPIError(output, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func writeAPIError(output http.ResponseWriter, status int, code, message string) {
	writeJSON(output, status, errorEnvelope{Error: apiError{Code: code, Message: message}})
}

func writeJSON(output http.ResponseWriter, status int, value any) {
	output.Header().Set("Cache-Control", "no-store")
	output.Header().Set("Content-Type", "application/json; charset=utf-8")
	output.WriteHeader(status)
	_ = json.NewEncoder(output).Encode(value)
}
