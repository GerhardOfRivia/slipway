package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/GerhardOfRivia/onderzeeer/internal/config"
	"github.com/GerhardOfRivia/onderzeeer/internal/queue"
)

func (server *Server) handleQueueSelection(output http.ResponseWriter, request *http.Request) {
	selector := request.URL.Query().Get("selector")
	instances, err := server.manager.selectQueues(selector)
	if err != nil {
		writeManagerError(output, err, false)
		return
	}
	writeJSON(output, http.StatusOK, instancesResponse{Instances: instances})
}

func (manager *Manager) selectQueues(selector string) ([]Instance, error) {
	if strings.TrimSpace(selector) == "" {
		return nil, fmt.Errorf("%w: queue selector is required", ErrNotFound)
	}
	if instance, err := manager.Get(selector); err == nil {
		return []Instance{instance}, nil
	} else if errors.Is(err, ErrAmbiguous) {
		return nil, err
	}
	path, err := absolutePath(selector)
	if err != nil {
		return nil, err
	}
	manager.mu.Lock()
	var snapshots []registration
	for _, runtime := range manager.instances {
		snapshots = append(snapshots, registration{view: cloneInstance(runtime.view), identity: runtime.configIdentity})
	}
	manager.mu.Unlock()
	var selected []Instance
	for _, item := range snapshots {
		equal := path == item.view.ConfigPath || path == item.identity
		if !equal {
			equal, err = config.PathsEquivalent(path, item.identity)
			if err != nil {
				return nil, err
			}
		}
		if equal || filepath.Dir(item.view.ConfigPath) == path {
			selected = append(selected, item.view)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("%w: no registered queue matches %q", ErrNotFound, selector)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].ConfigPath < selected[j].ConfigPath })
	return selected, nil
}

func (server *Server) handleQueueRead(output http.ResponseWriter, request *http.Request) {
	instance, err := server.manager.Get(request.PathValue("selector"))
	if err != nil {
		writeManagerError(output, err, false)
		return
	}
	store, err := queue.OpenReadOnlyContext(request.Context(), instance.DatabasePath)
	if err != nil {
		writeAPIError(output, http.StatusServiceUnavailable, "queue_unavailable", err)
		return
	}
	defer store.Close()
	ctx := request.Context()
	values := request.URL.Query()
	var result any
	switch request.PathValue("operation") {
	case "counts":
		result, err = store.Counts(ctx)
	case "jobs":
		limit, parseErr := strconv.Atoi(values.Get("limit"))
		offset, offsetErr := strconv.Atoi(values.Get("offset"))
		if parseErr != nil || offsetErr != nil || limit <= 0 || offset < 0 {
			writeAPIError(output, http.StatusBadRequest, "invalid_request", errors.New("positive limit and nonnegative offset required"))
			return
		}
		result, err = store.ListJobs(ctx, queue.JobFilter{Status: queue.Status(values.Get("status")), WatchName: values.Get("watch"), Limit: limit, Offset: offset})
	case "job", "runs", "commands":
		id, parseErr := strconv.ParseInt(values.Get("id"), 10, 64)
		if parseErr != nil || id <= 0 {
			writeAPIError(output, http.StatusBadRequest, "invalid_request", errors.New("positive record id required"))
			return
		}
		switch request.PathValue("operation") {
		case "job":
			result, err = store.GetJob(ctx, id)
		case "runs":
			result, err = store.ListRuns(ctx, id)
		case "commands":
			result, err = store.ListCommands(ctx, id)
		}
	default:
		writeAPIError(output, http.StatusNotFound, "not_found", errors.New("unknown queue operation"))
		return
	}
	if err != nil {
		status, code := http.StatusServiceUnavailable, "queue_unavailable"
		if errors.Is(err, queue.ErrNotFound) {
			status, code = http.StatusNotFound, "record_not_found"
		}
		writeAPIError(output, status, code, err)
		return
	}
	writeJSON(output, http.StatusOK, result)
}

// SelectQueues resolves names, IDs, and config paths entirely in the daemon.
func (client *Client) SelectQueues(ctx context.Context, selector string) ([]Instance, error) {
	var response instancesResponse
	err := client.doJSON(ctx, http.MethodGet, "/v1/queues?"+url.Values{"selector": {selector}}.Encode(), nil, &response)
	return response.Instances, err
}

// QueueReader exposes read-only history over the control socket. It owns its
// client, so Close can release all idle connections after an inspection command.
type QueueReader struct {
	client *Client
	id     string
}

func NewQueueReader(socket, id string) *QueueReader {
	return &QueueReader{client: NewClient(socket), id: id}
}

func (reader *QueueReader) Close() error {
	reader.client.CloseIdleConnections()
	return nil
}

func (reader *QueueReader) read(ctx context.Context, operation string, values url.Values, result any) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Captured command output may exceed the ordinary control response limit.
	// Decode it as a stream instead of holding a second full response buffer.
	path := "/v1/queues/" + url.PathEscape(reader.id) + "/" + operation + "?" + values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://onderzeeer"+path, nil)
	if err != nil {
		return err
	}
	response, err := reader.client.do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		err := decodeAPIError(response)
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Code == "record_not_found" {
			return fmt.Errorf("%w: %s", queue.ErrNotFound, apiErr.Message)
		}
		return err
	}
	return json.NewDecoder(response.Body).Decode(result)
}

func (reader *QueueReader) Counts(ctx context.Context) (queue.QueueCounts, error) {
	var result queue.QueueCounts
	err := reader.read(ctx, "counts", nil, &result)
	return result, err
}

func (reader *QueueReader) ListJobs(ctx context.Context, filter queue.JobFilter) ([]queue.Job, error) {
	var result []queue.Job
	values := url.Values{"status": {string(filter.Status)}, "watch": {filter.WatchName}, "limit": {strconv.Itoa(filter.Limit)}, "offset": {strconv.Itoa(filter.Offset)}}
	err := reader.read(ctx, "jobs", values, &result)
	return result, err
}

func (reader *QueueReader) GetJob(ctx context.Context, id int64) (*queue.Job, error) {
	var result queue.Job
	err := reader.read(ctx, "job", url.Values{"id": {strconv.FormatInt(id, 10)}}, &result)
	return &result, err
}

func (reader *QueueReader) ListRuns(ctx context.Context, id int64) ([]queue.Run, error) {
	var result []queue.Run
	err := reader.read(ctx, "runs", url.Values{"id": {strconv.FormatInt(id, 10)}}, &result)
	return result, err
}

func (reader *QueueReader) ListCommands(ctx context.Context, id int64) ([]queue.CommandExecution, error) {
	var result []queue.CommandExecution
	err := reader.read(ctx, "commands", url.Values{"id": {strconv.FormatInt(id, 10)}}, &result)
	return result, err
}
