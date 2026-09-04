package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	apiVersion              = "v1"
	maxRequestBodyBytes     = 1 << 20
	maxConfigsPerStart      = 1024
	defaultShutdownTimeout  = 30 * time.Second
	staleSocketProbeTimeout = 250 * time.Millisecond
)

var (
	// ErrDaemonAlreadyRunning means another server owns the selected control
	// socket. It is safe for callers to use errors.Is with this value.
	ErrDaemonAlreadyRunning = errors.New("control: daemon already running")
	// ErrServerClosed means Serve was called after Close or more than once.
	ErrServerClosed = errors.New("control: server is closed")
)

// Server exposes a Manager through a versioned HTTP/JSON API on a Unix socket.
// NewServer acquires the socket immediately, before any instance bootstrap can
// occur, so a second daemon cannot race the first daemon's startup.
type Server struct {
	manager *Manager
	logger  *slog.Logger
	path    string

	mu         sync.Mutex
	listener   net.Listener
	httpServer *http.Server
	lockFile   *os.File
	socketInfo os.FileInfo
	closed     bool
	served     bool

	networkOnce  sync.Once
	networkErr   error
	finalizeOnce sync.Once
	finalizeErr  error
}

// NewServer safely acquires socketPath and constructs a control server. The
// socket's immediate parent is private (0700), and the socket itself is 0600.
// Existing regular files and other non-socket filesystem entries are never
// removed. An unresponsive socket is removed only when a probe proves that no
// listener owns it.
func NewServer(socketPath string, manager *Manager, logger *slog.Logger) (*Server, error) {
	if manager == nil {
		return nil, errors.New("control: manager is required")
	}
	path, err := ResolveSocketPath(socketPath)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("control: socket path is required")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if err := ensurePrivateSocketDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	lockFile, err := acquireSocketLock(path + ".lock")
	if err != nil {
		return nil, err
	}
	cleanupLock := true
	defer func() {
		if cleanupLock {
			_ = releaseSocketLock(lockFile)
		}
	}()

	if err := prepareSocketPath(path); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("control: listen on %s: %w", path, err)
	}
	if unixListener, ok := listener.(*net.UnixListener); ok {
		// Close must not unlink a different socket that was swapped into the
		// pathname after acquisition. Server.Close performs an identity check
		// before removing the socket we actually own.
		unixListener.SetUnlinkOnClose(false)
	}
	cleanupListener := true
	defer func() {
		if cleanupListener {
			_ = listener.Close()
			_ = os.Remove(path)
		}
	}()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("control: protect socket %s: %w", path, err)
	}
	socketInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("control: inspect socket %s: %w", path, err)
	}

	server := &Server{
		manager:    manager,
		logger:     logger,
		path:       path,
		listener:   listener,
		lockFile:   lockFile,
		socketInfo: socketInfo,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/instances", server.handleStart)
	mux.HandleFunc("GET /v1/instances", server.handleList)
	mux.HandleFunc("POST /v1/instances/{selector}/stop", server.handleStop)
	mux.HandleFunc("POST /v1/run", server.handleRun)
	server.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       time.Minute,
	}

	cleanupLock = false
	cleanupListener = false
	return server, nil
}

// ResolveSocketPath resolves the control socket using explicit, environment,
// and per-user defaults, in that order.
func ResolveSocketPath(explicit string) (string, error) {
	if value := strings.TrimSpace(explicit); value != "" {
		return filepath.Clean(value), nil
	}
	if value := strings.TrimSpace(os.Getenv("SLIPWAY_SOCKET")); value != "" {
		return filepath.Clean(value), nil
	}
	if runtimeDirectory := strings.TrimSpace(os.Getenv("XDG_RUNTIME_DIR")); runtimeDirectory != "" {
		return filepath.Join(runtimeDirectory, "slipway", "slipway.sock"), nil
	}
	if cacheDirectory, err := os.UserCacheDir(); err == nil && strings.TrimSpace(cacheDirectory) != "" {
		return filepath.Join(cacheDirectory, "slipway", "slipway.sock"), nil
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf("slipway-%d", os.Geteuid()), "slipway.sock"), nil
}

// SocketPath is a convenience wrapper for callers that cannot return a path
// resolution error. The current resolution strategy always has a safe
// temporary fallback; ResolveSocketPath is preferred for forward compatibility.
func SocketPath(explicit string) string {
	path, _ := ResolveSocketPath(explicit)
	return path
}

// Path returns the resolved Unix socket path owned by the server.
func (server *Server) Path() string {
	if server == nil {
		return ""
	}
	return server.path
}

// Serve accepts requests until ctx is canceled, Close is called, or the HTTP
// server fails. Every exit path asks the manager to stop its instances before
// releasing the socket.
func (server *Server) Serve(ctx context.Context) error {
	if server == nil {
		return errors.New("control: server is required")
	}
	if ctx == nil {
		return errors.New("control: serve context is required")
	}

	server.mu.Lock()
	if server.closed || server.listener == nil {
		server.mu.Unlock()
		return ErrServerClosed
	}
	if server.served {
		server.mu.Unlock()
		return errors.New("control: Serve may only be called once")
	}
	server.served = true
	listener := server.listener
	httpServer := server.httpServer
	server.mu.Unlock()

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- httpServer.Serve(listener)
	}()

	var serveErr error
	select {
	case serveErr = <-serveDone:
	case <-ctx.Done():
	}

	networkErr := server.closeNetwork()
	shutdownContext, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	managerErr := server.manager.Shutdown(shutdownContext)

	if serveErr == nil {
		select {
		case serveErr = <-serveDone:
		case <-shutdownContext.Done():
			serveErr = shutdownContext.Err()
		}
	}
	var finalizeErr error
	if managerErr == nil {
		finalizeErr = server.releaseOwnership()
	}

	if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, networkErr, managerErr, finalizeErr)
}

// Close stops accepting requests, asks the manager to stop every instance,
// and only then releases the socket and ownership lock. If shutdown times out,
// ownership is deliberately retained so another daemon cannot overlap the old
// runtimes; a later Close call may finish cleanup. Close is safe to call
// repeatedly and concurrently, including before Serve.
func (server *Server) Close() error {
	if server == nil {
		return nil
	}
	networkErr := server.closeNetwork()
	shutdownContext, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	managerErr := server.manager.Shutdown(shutdownContext)
	if managerErr != nil {
		return errors.Join(networkErr, managerErr)
	}
	return errors.Join(networkErr, server.releaseOwnership())
}

func (server *Server) closeNetwork() error {
	server.networkOnce.Do(func() {
		server.mu.Lock()
		server.closed = true
		httpServer := server.httpServer
		listener := server.listener
		server.mu.Unlock()

		var results []error
		if httpServer != nil {
			if err := httpServer.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				results = append(results, err)
			}
		}
		if listener != nil {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				results = append(results, err)
			}
		}
		server.networkErr = errors.Join(results...)
	})
	return server.networkErr
}

func (server *Server) releaseOwnership() error {
	server.finalizeOnce.Do(func() {
		server.finalizeErr = errors.Join(
			removeOwnedSocket(server.path, server.socketInfo),
			releaseSocketLock(server.lockFile),
		)
	})
	return server.finalizeErr
}

type startRequest struct {
	ConfigPaths []string `json:"config_paths,omitempty"`
	ConfigPath  string   `json:"config_path,omitempty"`
	Name        string   `json:"name,omitempty"`
}

type runRequest struct {
	ConfigPaths  []string `json:"config_paths,omitempty"`
	ConfigPath   string   `json:"config_path,omitempty"`
	Name         string   `json:"name,omitempty"`
	RemoveOnExit bool     `json:"remove_on_exit,omitempty"`
}

func (request runRequest) paths() ([]string, error) {
	return (startRequest{
		ConfigPaths: request.ConfigPaths,
		ConfigPath:  request.ConfigPath,
	}).paths()
}

func (request startRequest) paths() ([]string, error) {
	paths := append([]string(nil), request.ConfigPaths...)
	if strings.TrimSpace(request.ConfigPath) != "" {
		if len(paths) != 0 {
			return nil, errors.New("provide config_path or config_paths, not both")
		}
		paths = []string{request.ConfigPath}
	}
	if len(paths) == 0 {
		return nil, errors.New("at least one configuration path is required")
	}
	if len(paths) > maxConfigsPerStart {
		return nil, fmt.Errorf("at most %d configuration paths may be started at once", maxConfigsPerStart)
	}
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			return nil, errors.New("configuration paths must not be empty")
		}
	}
	return paths, nil
}

type instancesResponse struct {
	Instances []Instance `json:"instances"`
}

type instanceResponse struct {
	Instance Instance `json:"instance"`
}

type errorBody struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// RunEvent is one record in the newline-delimited stream returned by /v1/run.
type RunEvent struct {
	Type     string   `json:"type"`
	Instance Instance `json:"instance,omitzero"`
	Log      string   `json:"log,omitempty"`
	Error    string   `json:"error,omitempty"`
}

func (server *Server) handleStart(output http.ResponseWriter, request *http.Request) {
	var input startRequest
	if err := decodeRequest(output, request, &input); err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err)
		return
	}
	paths, err := input.paths()
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err)
		return
	}
	instances, err := server.manager.StartManyContext(request.Context(), paths, input.Name)
	if err != nil {
		writeManagerError(output, err, true)
		return
	}
	writeJSON(output, http.StatusCreated, instancesResponse{Instances: instances})
}

func (server *Server) handleList(output http.ResponseWriter, request *http.Request) {
	all := false
	if value := request.URL.Query().Get("all"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			writeAPIError(output, http.StatusBadRequest, "invalid_request", fmt.Errorf("invalid all value %q", value))
			return
		}
		all = parsed
	}
	writeJSON(output, http.StatusOK, instancesResponse{Instances: server.manager.List(all)})
}

func (server *Server) handleStop(output http.ResponseWriter, request *http.Request) {
	selector := request.PathValue("selector")
	if strings.TrimSpace(selector) == "" {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", errors.New("instance selector is required"))
		return
	}
	instance, err := server.manager.Stop(request.Context(), selector)
	if err != nil {
		writeManagerError(output, err, false)
		return
	}
	writeJSON(output, http.StatusOK, instanceResponse{Instance: instance})
}

func (server *Server) handleRun(output http.ResponseWriter, request *http.Request) {
	var input runRequest
	if err := decodeRequest(output, request, &input); err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err)
		return
	}
	paths, err := input.paths()
	if err != nil {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", err)
		return
	}
	if len(paths) != 1 {
		writeAPIError(output, http.StatusBadRequest, "invalid_request", errors.New("run requires exactly one configuration path"))
		return
	}
	flusher, ok := output.(http.Flusher)
	if !ok {
		writeAPIError(output, http.StatusInternalServerError, "streaming_unsupported", errors.New("HTTP streaming is unavailable"))
		return
	}
	instance, attachment, err := server.manager.startAttachedContext(request.Context(), paths[0], input.Name, input.RemoveOnExit)
	if err != nil {
		writeManagerError(output, err, true)
		return
	}
	defer attachment.Cancel()

	output.Header().Set("Content-Type", "application/x-ndjson")
	output.Header().Set("X-slipway-API-Version", apiVersion)
	output.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(output)
	if err := encoder.Encode(RunEvent{Type: "started", Instance: instance}); err != nil {
		return
	}
	flusher.Flush()

	type waitResult struct {
		instance Instance
		err      error
	}
	waitDone := make(chan waitResult, 1)
	go func() {
		finished, waitErr := attachment.Wait(request.Context())
		waitDone <- waitResult{instance: finished, err: waitErr}
	}()

	lines := attachment.Lines()
	for {
		select {
		case <-request.Context().Done():
			// The manager owns the runtime context. A dropped attachment only
			// removes this subscriber and never stops the instance.
			return
		case line, open := <-lines:
			if !open {
				lines = nil
				continue
			}
			if err := encoder.Encode(RunEvent{Type: "log", Log: line}); err != nil {
				return
			}
			flusher.Flush()
		case result := <-waitDone:
			if result.err != nil {
				if errors.Is(result.err, context.Canceled) && request.Context().Err() != nil {
					return
				}
				_ = encoder.Encode(RunEvent{Type: "error", Error: result.err.Error()})
				flusher.Flush()
				return
			}
			for lines != nil {
				select {
				case line, open := <-lines:
					if !open {
						lines = nil
						continue
					}
					if err := encoder.Encode(RunEvent{Type: "log", Log: line}); err != nil {
						return
					}
				default:
					lines = nil
				}
			}
			if err := encoder.Encode(RunEvent{Type: "exited", Instance: result.instance}); err != nil {
				return
			}
			flusher.Flush()
			return
		}
	}
}

func decodeRequest(output http.ResponseWriter, request *http.Request, destination any) error {
	request.Body = http.MaxBytesReader(output, request.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
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

func writeManagerError(output http.ResponseWriter, err error, start bool) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeAPIError(output, http.StatusNotFound, "not_found", err)
	case errors.Is(err, ErrAmbiguous):
		writeAPIError(output, http.StatusConflict, "ambiguous_selector", err)
	case errors.Is(err, ErrAlreadyActive):
		writeAPIError(output, http.StatusConflict, "already_active", err)
	case errors.Is(err, ErrNameInUse):
		writeAPIError(output, http.StatusConflict, "name_in_use", err)
	case errors.Is(err, ErrNotActive):
		writeAPIError(output, http.StatusConflict, "not_active", err)
	case errors.Is(err, ErrShuttingDown):
		writeAPIError(output, http.StatusServiceUnavailable, "shutting_down", err)
	case start:
		writeAPIError(output, http.StatusUnprocessableEntity, "start_failed", err)
	default:
		writeAPIError(output, http.StatusInternalServerError, "internal_error", err)
	}
}

func writeAPIError(output http.ResponseWriter, status int, code string, err error) {
	message := http.StatusText(status)
	if err != nil {
		message = err.Error()
	}
	writeJSON(output, status, errorBody{Error: apiError{Code: code, Message: message}})
}

func writeJSON(output http.ResponseWriter, status int, value any) {
	output.Header().Set("Content-Type", "application/json")
	output.Header().Set("X-slipway-API-Version", apiVersion)
	output.WriteHeader(status)
	_ = json.NewEncoder(output).Encode(value)
}

func ensurePrivateSocketDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return fmt.Errorf("control: create socket directory %s: %w", directory, err)
		}
		if err := os.Chmod(directory, 0o700); err != nil {
			return fmt.Errorf("control: protect socket directory %s: %w", directory, err)
		}
		info, err = os.Lstat(directory)
	}
	if err != nil {
		return fmt.Errorf("control: inspect socket directory %s: %w", directory, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("control: socket parent %s is not a directory", directory)
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf("control: socket parent %s must have mode 0700 (has %04o)", directory, info.Mode().Perm())
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("control: socket parent %s is not owned by uid %d", directory, os.Geteuid())
	}
	return nil
}

func acquireSocketLock(filename string) (*os.File, error) {
	if info, err := os.Lstat(filename); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("control: socket lock %s is not a regular file", filename)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("control: inspect socket lock %s: %w", filename, err)
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("control: open socket lock %s: %w", filename, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w at %s", ErrDaemonAlreadyRunning, strings.TrimSuffix(filename, ".lock"))
		}
		return nil, fmt.Errorf("control: lock socket %s: %w", filename, err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("control: protect socket lock %s: %w", filename, err)
	}
	return file, nil
}

func releaseSocketLock(file *os.File) error {
	if file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(unlockErr, closeErr)
}

func prepareSocketPath(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("control: inspect socket %s: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("control: refusing to replace non-socket path %s", path)
	}

	connection, dialErr := net.DialTimeout("unix", path, staleSocketProbeTimeout)
	if dialErr == nil {
		_ = connection.Close()
		return fmt.Errorf("%w at %s", ErrDaemonAlreadyRunning, path)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
		return fmt.Errorf("control: cannot prove existing socket %s is stale: %w", path, dialErr)
	}

	current, statErr := os.Lstat(path)
	if errors.Is(statErr, os.ErrNotExist) {
		return nil
	}
	if statErr != nil {
		return fmt.Errorf("control: re-inspect stale socket %s: %w", path, statErr)
	}
	if current.Mode()&os.ModeSocket == 0 || !os.SameFile(info, current) {
		return fmt.Errorf("control: socket %s changed while checking whether it was stale", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("control: remove stale socket %s: %w", path, err)
	}
	return nil
}

func removeOwnedSocket(path string, owned os.FileInfo) error {
	if path == "" || owned == nil {
		return nil
	}
	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("control: inspect owned socket %s: %w", path, err)
	}
	if current.Mode()&os.ModeSocket == 0 || !os.SameFile(owned, current) {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("control: remove socket %s: %w", path, err)
	}
	return nil
}
