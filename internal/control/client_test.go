package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestClientDoesNotCallUnknownRequestOutcomeDaemonUnavailable(t *testing.T) {
	t.Parallel()
	transportFailure := errors.New("connection reset after request write")
	client := &Client{
		socketPath: "/private/slipway.sock",
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, transportFailure
		})},
	}

	_, err := client.List(context.Background(), false)
	if !errors.Is(err, transportFailure) {
		t.Fatalf("List error = %v, want wrapped transport failure", err)
	}
	if errors.Is(err, ErrDaemonUnavailable) {
		t.Fatalf("unknown request outcome was mislabeled daemon unavailable: %v", err)
	}
	if strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("List error = %q, want no mutation outcome warning", err)
	}
}

func TestClientMarksInterruptedMutationOutcomeUnknown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		socketPath: "/private/slipway.sock",
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			cancel()
			return nil, context.Canceled
		})},
	}

	_, err := client.Start(ctx, []string{"/configs/one.yaml"}, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want wrapped cancellation", err)
	}
	if !strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("Start error = %q, want outcome warning", err)
	}
}

func TestClientMarksTruncatedSuccessfulMutationOutcomeUnknown(t *testing.T) {
	t.Parallel()
	client := clientWithSuccessfulBody(http.StatusCreated, `{"instances":[`)

	_, err := client.Start(context.Background(), []string{"/configs/one.yaml"}, "")
	if err == nil {
		t.Fatal("Start succeeded with a truncated success response")
	}
	if !strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("Start error = %q, want outcome warning", err)
	}
	if !strings.Contains(err.Error(), "decode daemon response") {
		t.Fatalf("Start error = %q, want response decoding context", err)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Start error = %v, want wrapped unexpected EOF", err)
	}
}

func TestClientLeavesTruncatedSuccessfulGETOutcomeOrdinary(t *testing.T) {
	t.Parallel()
	client := clientWithSuccessfulBody(http.StatusOK, `{"instances":[`)

	_, err := client.List(context.Background(), false)
	if err == nil {
		t.Fatal("List succeeded with a truncated success response")
	}
	if strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("List error = %q, want no mutation outcome warning", err)
	}
	if !strings.Contains(err.Error(), "decode daemon response") {
		t.Fatalf("List error = %q, want response decoding context", err)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("List error = %v, want wrapped unexpected EOF", err)
	}
}

func TestClientRejectsOversizedSuccessfulMutationResponse(t *testing.T) {
	t.Parallel()
	client := clientWithSuccessfulBody(http.StatusCreated, strings.Repeat(" ", maxResponseBodyBytes+1))

	_, err := client.Start(context.Background(), []string{"/configs/one.yaml"}, "")
	if err == nil {
		t.Fatal("Start succeeded with an oversized success response")
	}
	if !strings.Contains(err.Error(), "outcome may be unknown") || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Start error = %q, want size and mutation-outcome warnings", err)
	}
}

func TestClientRunWarnsWhenStreamEndsBeforeStartAcknowledgment(t *testing.T) {
	t.Parallel()
	client := clientWithSuccessfulBody(http.StatusOK, "")

	instance, err := client.Run(context.Background(), "/configs/one.yaml", "", nil)
	if err == nil || !strings.Contains(err.Error(), "outcome may be unknown") || !strings.Contains(err.Error(), "may still be running") {
		t.Fatalf("Run error = %v, want unacknowledged-instance warning", err)
	}
	if instance.ID != "" {
		t.Fatalf("Run instance before acknowledgment = %+v", instance)
	}
}

func TestClientRunKeepsAcknowledgedStreamFailureOrdinary(t *testing.T) {
	t.Parallel()
	client := clientWithSuccessfulBody(http.StatusOK, `{"type":"started","instance":{"id":"abc123def456","state":"running"}}`+"\n")

	instance, err := client.Run(context.Background(), "/configs/one.yaml", "", nil)
	if err == nil {
		t.Fatal("Run succeeded without an exited event")
	}
	if strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("Run error after acknowledgment = %q, want ordinary attachment error", err)
	}
	if instance.ID != "abc123def456" {
		t.Fatalf("Run acknowledged instance = %+v", instance)
	}
}

func TestClientRunSendsRemoveOnExit(t *testing.T) {
	t.Parallel()
	client := &Client{
		socketPath: "/private/slipway.sock",
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPost || request.URL.Path != "/v1/run" {
				t.Fatalf("Run request = %s %s, want POST /v1/run", request.Method, request.URL.Path)
			}
			var input struct {
				ConfigPath   string `json:"config_path"`
				Name         string `json:"name"`
				RemoveOnExit bool   `json:"remove_on_exit"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatalf("decode Run request: %v", err)
			}
			if input.ConfigPath != "/configs/one.yaml" || input.Name != "ephemeral" || !input.RemoveOnExit {
				t.Fatalf("Run request body = %+v", input)
			}
			body := `{"type":"started","instance":{"id":"abc123def456","state":"running"}}` + "\n" +
				`{"type":"exited","instance":{"id":"abc123def456","state":"exited"}}` + "\n"
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	finished, err := client.RunWithOptions(context.Background(), "/configs/one.yaml", "ephemeral", RunOptions{RemoveOnExit: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if finished.ID != "abc123def456" || finished.State != StateExited {
		t.Fatalf("Run result = %+v", finished)
	}
}

func TestClientRunOmitsDefaultRemoveOnExit(t *testing.T) {
	t.Parallel()
	client := &Client{
		socketPath: "/private/slipway.sock",
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			var input map[string]json.RawMessage
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatalf("decode Run request: %v", err)
			}
			if _, exists := input["remove_on_exit"]; exists {
				t.Fatalf("default Run request unexpectedly contains remove_on_exit: %s", input["remove_on_exit"])
			}
			body := `{"type":"started","instance":{"id":"abc123def456","state":"running"}}` + "\n" +
				`{"type":"exited","instance":{"id":"abc123def456","state":"exited"}}` + "\n"
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		})},
	}

	if _, err := client.Run(context.Background(), "/configs/one.yaml", "retained", nil); err != nil {
		t.Fatal(err)
	}
}

func clientWithSuccessfulBody(status int, body string) *Client {
	return &Client{
		socketPath: "/private/slipway.sock",
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		})},
	}
}
