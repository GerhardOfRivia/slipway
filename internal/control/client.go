package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponseBodyBytes = 4 << 20

var ErrDaemonUnavailable = errors.New("control: daemon unavailable")

// DaemonUnavailableError reports the Unix socket that could not be reached.
// It matches ErrDaemonUnavailable through errors.Is.
type DaemonUnavailableError struct {
	SocketPath string
	Err        error
}

func (err *DaemonUnavailableError) Error() string {
	if err == nil {
		return ErrDaemonUnavailable.Error()
	}
	if err.Err == nil {
		return fmt.Sprintf("slipway daemon is unavailable at %s; start it with `slipwayd`", err.SocketPath)
	}
	return fmt.Sprintf("slipway daemon is unavailable at %s: %v; start it with `slipwayd`", err.SocketPath, err.Err)
}

func (err *DaemonUnavailableError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

func (err *DaemonUnavailableError) Is(target error) bool {
	return target == ErrDaemonUnavailable
}

// APIError is a structured non-success response from the daemon.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (err *APIError) Error() string {
	if err == nil {
		return "slipway daemon request failed"
	}
	if err.Code == "" {
		return fmt.Sprintf("slipway daemon request failed (%s): %s", http.StatusText(err.StatusCode), err.Message)
	}
	return fmt.Sprintf("slipway daemon request failed (%s): %s", err.Code, err.Message)
}

// Client is a standard-library HTTP client configured to dial one Unix
// control socket. It is safe for concurrent use.
type Client struct {
	socketPath string
	httpClient *http.Client
	transport  *http.Transport
}

// NewClient constructs a client. An empty socketPath uses ResolveSocketPath's
// environment and per-user defaults.
func NewClient(socketPath string) *Client {
	resolved, err := ResolveSocketPath(socketPath)
	if err != nil {
		// Resolution currently always has a temporary fallback. Retaining the
		// explicit value here makes a future resolution failure surface as a
		// clear dial error rather than a panic in this no-error constructor.
		resolved = socketPath
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		DisableCompression: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			connection, dialErr := dialer.DialContext(ctx, "unix", resolved)
			if dialErr != nil {
				return nil, &DaemonUnavailableError{SocketPath: resolved, Err: dialErr}
			}
			return connection, nil
		},
	}
	return &Client{
		socketPath: resolved,
		transport:  transport,
		httpClient: &http.Client{Transport: transport},
	}
}

// SocketPath returns the resolved socket used by this client.
func (client *Client) SocketPath() string {
	if client == nil {
		return ""
	}
	return client.socketPath
}

// CloseIdleConnections releases idle Unix connections retained by the HTTP
// transport. It does not affect a Run call that is currently streaming.
func (client *Client) CloseIdleConnections() {
	if client != nil && client.transport != nil {
		client.transport.CloseIdleConnections()
	}
}

// Start starts one or more configurations and returns once the manager has
// registered them. The instances continue independently of this request.
func (client *Client) Start(ctx context.Context, configPaths []string, name string) ([]Instance, error) {
	var response instancesResponse
	err := client.doJSON(ctx, http.MethodPost, "/v1/instances", startRequest{
		ConfigPaths: append([]string(nil), configPaths...),
		Name:        name,
	}, &response)
	if err != nil {
		return nil, err
	}
	return response.Instances, nil
}

// List returns active instances by default, or all retained instances when all
// is true.
func (client *Client) List(ctx context.Context, all bool) ([]Instance, error) {
	query := url.Values{}
	query.Set("all", strconv.FormatBool(all))
	var response instancesResponse
	if err := client.doJSON(ctx, http.MethodGet, "/v1/instances?"+query.Encode(), nil, &response); err != nil {
		return nil, err
	}
	return response.Instances, nil
}

// Stop requests an explicit instance stop and waits for the manager's result.
func (client *Client) Stop(ctx context.Context, selector string) (Instance, error) {
	var response instanceResponse
	path := "/v1/instances/" + url.PathEscape(selector) + "/stop"
	if err := client.doJSON(ctx, http.MethodPost, path, struct{}{}, &response); err != nil {
		return Instance{}, err
	}
	return response.Instance, nil
}

// Run starts exactly one configuration and consumes its attachment stream.
// Returning from onEvent with an error, canceling ctx, or losing the connection
// detaches the client; none of those actions implicitly stops the instance.
func (client *Client) Run(
	ctx context.Context,
	configPath string,
	name string,
	onEvent func(RunEvent) error,
) (Instance, error) {
	request, err := client.newJSONRequest(ctx, http.MethodPost, "/v1/run", startRequest{
		ConfigPath: configPath,
		Name:       name,
	})
	if err != nil {
		return Instance{}, err
	}
	request.Header.Set("Accept", "application/x-ndjson")
	response, err := client.do(request)
	if err != nil {
		return Instance{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Instance{}, decodeAPIError(response)
	}

	decoder := json.NewDecoder(response.Body)
	var last Instance
	started := false
	for {
		var event RunEvent
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				if ctx.Err() != nil {
					return last, client.runAttachmentError(started, ctx.Err())
				}
				return last, client.runAttachmentError(started, errors.New("slipway daemon attachment ended before an exited event"))
			}
			if ctx.Err() != nil {
				return last, client.runAttachmentError(started, ctx.Err())
			}
			return last, client.runAttachmentError(started, fmt.Errorf("decode daemon event: %w", err))
		}
		switch event.Type {
		case "started":
			if started {
				return last, errors.New("slipway daemon sent more than one started event")
			}
			if event.Instance.ID == "" {
				return last, client.runAttachmentError(false, errors.New("slipway daemon sent a started event without an instance ID"))
			}
			started = true
			last = event.Instance
		case "log":
			if !started {
				return last, client.runAttachmentError(false, errors.New("slipway daemon sent a log event before started"))
			}
		case "exited":
			if !started {
				return last, client.runAttachmentError(false, errors.New("slipway daemon sent an exited event before started"))
			}
			last = event.Instance
		case "error":
			if event.Error == "" {
				event.Error = "daemon attachment failed"
			}
			return last, client.runAttachmentError(started, errors.New(event.Error))
		default:
			return last, client.runAttachmentError(started, fmt.Errorf("slipway daemon sent unknown event type %q", event.Type))
		}
		if onEvent != nil {
			if err := onEvent(event); err != nil {
				return last, err
			}
		}
		if event.Type == "exited" {
			return last, nil
		}
	}
}

func (client *Client) runAttachmentError(started bool, err error) error {
	if err == nil || started {
		return err
	}
	return fmt.Errorf(
		"slipway daemon attachment via %s failed before the instance ID was acknowledged; an instance may still be running and the outcome may be unknown: %w",
		client.socketPath,
		err,
	)
}

func (client *Client) doJSON(ctx context.Context, method, path string, input, output any) error {
	request, err := client.newJSONRequest(ctx, method, path, input)
	if err != nil {
		return err
	}
	response, err := client.do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return decodeAPIError(response)
	}
	contents, readErr := readBoundedBody(response.Body, maxResponseBodyBytes)
	if readErr != nil {
		return client.successResponseError(request, fmt.Errorf("read daemon response: %w", readErr))
	}
	if output == nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	if err := decoder.Decode(output); err != nil {
		return client.successResponseError(request, fmt.Errorf("decode daemon response: %w", err))
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return client.successResponseError(request, fmt.Errorf("decode daemon response: %w", err))
	}
	return nil
}

func (client *Client) successResponseError(request *http.Request, err error) error {
	if err == nil {
		return nil
	}
	if request == nil || !methodMayMutate(request.Method) {
		return err
	}
	return fmt.Errorf("slipway daemon returned an unreadable success response to %s via %s; outcome may be unknown: %w",
		request.Method, client.socketPath, err)
}

func (client *Client) newJSONRequest(ctx context.Context, method, path string, input any) (*http.Request, error) {
	if client == nil || client.httpClient == nil {
		return nil, errors.New("control: client is required")
	}
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("encode daemon request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://slipway"+path, body)
	if err != nil {
		return nil, fmt.Errorf("create daemon request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-slipway-API-Version", apiVersion)
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

func (client *Client) do(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err == nil {
		return response, nil
	}
	if request.Context().Err() != nil {
		if !methodMayMutate(request.Method) {
			return nil, request.Context().Err()
		}
		return nil, fmt.Errorf("slipway daemon request via %s was interrupted; outcome may be unknown: %w", client.socketPath, request.Context().Err())
	}
	var unavailable *DaemonUnavailableError
	if errors.As(err, &unavailable) {
		return nil, unavailable
	}
	if !methodMayMutate(request.Method) {
		return nil, fmt.Errorf("slipway daemon request via %s failed before a response: %w", client.socketPath, err)
	}
	return nil, fmt.Errorf("slipway daemon request via %s failed before a response; outcome may be unknown: %w", client.socketPath, err)
}

func methodMayMutate(method string) bool {
	return method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions
}

func readBoundedBody(body io.Reader, limit int64) ([]byte, error) {
	contents, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(contents)) > limit {
		return nil, fmt.Errorf("response exceeds %d-byte limit", limit)
	}
	return contents, nil
}

func decodeAPIError(response *http.Response) error {
	contents, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes))
	if readErr != nil {
		return fmt.Errorf("read daemon error response: %w", readErr)
	}
	var envelope errorBody
	if err := json.Unmarshal(contents, &envelope); err == nil && envelope.Error.Message != "" {
		return &APIError{
			StatusCode: response.StatusCode,
			Code:       envelope.Error.Code,
			Message:    envelope.Error.Message,
		}
	}
	message := strings.TrimSpace(string(contents))
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	return &APIError{StatusCode: response.StatusCode, Message: message}
}
