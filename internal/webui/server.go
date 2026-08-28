// Package webui exposes slipway's optional loopback-only web dashboard.
package webui

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GerhardOfRivia/slipway/internal/control"
)

const shutdownTimeout = 5 * time.Second

//go:embed dist/*
var embeddedAssets embed.FS

// Server serves an embedded single-page application and its narrow JSON API.
// It deliberately accepts loopback listeners only; remote access should use an
// authenticated tunnel or reverse proxy.
type Server struct {
	listener   net.Listener
	httpServer *http.Server
	address    string
	token      string
	tokenPath  string

	closeOnce sync.Once
	closeErr  error
}

// NewServer acquires address immediately and constructs a web server. The
// address must contain an explicit loopback host and TCP port.
func NewServer(address, tokenPath string, manager *control.Manager, logger *slog.Logger) (*Server, error) {
	if manager == nil {
		return nil, errors.New("webui: manager is required")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, errors.New("webui: listen address is required")
	}
	if err := validateLoopbackAddress(address); err != nil {
		return nil, err
	}
	tokenPath = strings.TrimSpace(tokenPath)
	if tokenPath == "" {
		return nil, errors.New("webui: token path is required")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("webui: listen on %s: %w", address, err)
	}
	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.IP == nil || !tcpAddress.IP.IsLoopback() {
		_ = listener.Close()
		return nil, fmt.Errorf("webui: resolved listen address %q is not loopback", address)
	}
	token, err := writeAccessToken(tokenPath)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	handler, err := newHandler(manager, logger, token)
	if err != nil {
		_ = listener.Close()
		removeAccessToken(tokenPath, token)
		return nil, err
	}
	server := &Server{
		listener:  listener,
		address:   listener.Addr().String(),
		token:     token,
		tokenPath: tokenPath,
	}
	server.httpServer = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    16 << 10,
	}
	return server, nil
}

func writeAccessToken(filename string) (string, error) {
	directory := filepath.Dir(filename)
	info, err := os.Stat(directory)
	if err != nil {
		return "", fmt.Errorf("webui: inspect token directory %s: %w", directory, err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("webui: token directory %s must be a private directory", directory)
	}

	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("webui: generate access token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(bytes)
	temporary, err := os.CreateTemp(directory, ".slipway-web-token-*")
	if err != nil {
		return "", fmt.Errorf("webui: create temporary token: %w", err)
	}
	temporaryName := temporary.Name()
	cleanup := true
	defer func() {
		_ = temporary.Close()
		if cleanup {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return "", fmt.Errorf("webui: protect temporary token: %w", err)
	}
	if _, err := temporary.WriteString(token + "\n"); err != nil {
		return "", fmt.Errorf("webui: write temporary token: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return "", fmt.Errorf("webui: sync temporary token: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("webui: close temporary token: %w", err)
	}
	if err := os.Rename(temporaryName, filename); err != nil {
		return "", fmt.Errorf("webui: install token %s: %w", filename, err)
	}
	cleanup = false
	return token, nil
}

func removeAccessToken(filename, token string) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return
	}
	current := strings.TrimSpace(string(contents))
	if subtle.ConstantTimeCompare([]byte(current), []byte(token)) == 1 {
		_ = os.Remove(filename)
	}
}

func validateLoopbackAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("webui: listen address must be host:port: %w", err)
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("webui: refusing non-loopback listen address %q; use an authenticated tunnel for remote access", address)
	}
	return nil
}

// Address returns the acquired TCP address.
func (server *Server) Address() string {
	if server == nil {
		return ""
	}
	return server.address
}

// TokenPath returns the private file containing the bearer token required by
// the JSON API.
func (server *Server) TokenPath() string {
	if server == nil {
		return ""
	}
	return server.tokenPath
}

// Serve handles requests until ctx is canceled, Close is called, or the HTTP
// server fails.
func (server *Server) Serve(ctx context.Context) error {
	if server == nil || server.listener == nil || server.httpServer == nil {
		return errors.New("webui: server is required")
	}
	if ctx == nil {
		return errors.New("webui: serve context is required")
	}

	done := make(chan error, 1)
	go func() {
		done <- server.httpServer.Serve(server.listener)
	}()

	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := server.httpServer.Shutdown(shutdownContext)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.httpServer.Close())
	}
	serveErr := <-done
	if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
		serveErr = nil
	}
	return errors.Join(shutdownErr, serveErr)
}

// Close releases the listener. It is safe before Serve and may be repeated.
func (server *Server) Close() error {
	if server == nil {
		return nil
	}
	server.closeOnce.Do(func() {
		var errs []error
		if server.httpServer != nil {
			if err := server.httpServer.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		if server.listener != nil {
			if err := server.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		server.closeErr = errors.Join(errs...)
		removeAccessToken(server.tokenPath, server.token)
	})
	return server.closeErr
}

func newHandler(manager *control.Manager, logger *slog.Logger, token string) (http.Handler, error) {
	dist, err := fs.Sub(embeddedAssets, "dist")
	if err != nil {
		return nil, fmt.Errorf("webui: open embedded assets: %w", err)
	}
	index, err := fs.ReadFile(dist, "index.html")
	if err != nil {
		return nil, fmt.Errorf("webui: read embedded index: %w", err)
	}
	files := http.FileServer(http.FS(dist))
	api := apiServer{manager: manager, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/queues", api.handleQueues)
	mux.HandleFunc("GET /api/v1/queues/{queueID}/jobs", api.handleJobs)
	mux.HandleFunc("GET /api/v1/queues/{queueID}/jobs/{jobID}", api.handleJob)
	mux.HandleFunc("GET /api/v1/queues/{queueID}/commands/{commandID}/output", api.handleCommandOutput)
	mux.HandleFunc("GET /api/v1/instances", api.handleInstances)
	mux.HandleFunc("POST /api/v1/queues/{queueID}/start", api.handleStart)
	mux.HandleFunc("POST /api/v1/instances/{instanceID}/stop", api.handleStop)
	mux.HandleFunc("/", func(output http.ResponseWriter, request *http.Request) {
		if isAPIPath(request.URL.Path) {
			writeAPIError(output, http.StatusNotFound, "not_found", "API endpoint not found")
			return
		}
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			output.Header().Set("Allow", "GET, HEAD")
			http.Error(output, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		cleaned := strings.TrimPrefix(path.Clean(request.URL.Path), "/")
		if cleaned != "." && cleaned != "" {
			if info, statErr := fs.Stat(dist, cleaned); statErr == nil && !info.IsDir() {
				if strings.HasPrefix(cleaned, "assets/") {
					output.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				files.ServeHTTP(output, request)
				return
			}
			if strings.HasPrefix(cleaned, "assets/") {
				http.NotFound(output, request)
				return
			}
		}
		output.Header().Set("Cache-Control", "no-cache")
		output.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = output.Write(index)
	})

	return securityHeaders(validateHost(requireAPIAuth(token, mux))), nil
}

func requireAPIAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(output http.ResponseWriter, request *http.Request) {
		if isAPIPath(request.URL.Path) {
			provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
			if provided == request.Header.Get("Authorization") ||
				subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
				output.Header().Set("WWW-Authenticate", `Bearer realm="slipway-web"`)
				writeAPIError(output, http.StatusUnauthorized, "unauthorized", "A valid slipway web token is required")
				return
			}
		}
		next.ServeHTTP(output, request)
	})
}

func isAPIPath(requestPath string) bool {
	return requestPath == "/api" || strings.HasPrefix(requestPath, "/api/")
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(output http.ResponseWriter, request *http.Request) {
		output.Header().Set("Content-Security-Policy", "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; img-src 'self' data:; object-src 'none'; script-src 'self'; style-src 'self'")
		output.Header().Set("Referrer-Policy", "no-referrer")
		output.Header().Set("X-Content-Type-Options", "nosniff")
		output.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(output, request)
	})
}

func validateHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(output http.ResponseWriter, request *http.Request) {
		host := request.Host
		if parsed, _, err := net.SplitHostPort(host); err == nil {
			host = parsed
		}
		host = strings.Trim(host, "[]")
		ip := net.ParseIP(host)
		if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
			http.Error(output, "unrecognized host", http.StatusMisdirectedRequest)
			return
		}
		next.ServeHTTP(output, request)
	})
}
