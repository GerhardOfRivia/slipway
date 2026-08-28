package control

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
)

const (
	defaultLogCapacity         = 256
	defaultLogSubscriberBuffer = 32
	defaultMaxLogLineBytes     = 64 * 1024
	defaultMaxLogBytes         = 256 * 1024
)

// LogStream is a bounded line-oriented io.Writer. It retains recent lines and
// broadcasts new lines to cancellable subscribers without allowing a slow
// subscriber to block an instance.
type LogStream struct {
	mu               sync.Mutex
	capacity         int
	subscriberBuffer int
	maxLineBytes     int
	maxBytes         int
	retainedBytes    int
	lines            []string
	partial          []byte
	truncated        bool
	subscribers      map[uint64]chan string
	nextSubscriberID uint64
	closed           bool
}

// NewLogStream constructs a bounded log stream. Non-positive values select
// production defaults.
func NewLogStream(capacity, subscriberBuffer, maxLineBytes int) *LogStream {
	return newLogStream(capacity, subscriberBuffer, maxLineBytes, 0)
}

func newLogStream(capacity, subscriberBuffer, maxLineBytes, maxBytes int) *LogStream {
	if capacity <= 0 {
		capacity = defaultLogCapacity
	}
	if subscriberBuffer <= 0 {
		subscriberBuffer = defaultLogSubscriberBuffer
	}
	if maxLineBytes <= 0 {
		maxLineBytes = defaultMaxLogLineBytes
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxLogBytes
	}
	return &LogStream{
		capacity:         capacity,
		subscriberBuffer: subscriberBuffer,
		maxLineBytes:     maxLineBytes,
		maxBytes:         maxBytes,
		lines:            make([]string, 0, capacity),
		subscribers:      make(map[uint64]chan string),
	}
}

// Write splits text into lines. Lines longer than maxLineBytes are truncated,
// and incomplete final lines are flushed by Close.
func (stream *LogStream) Write(value []byte) (int, error) {
	if stream == nil {
		return len(value), nil
	}
	written := len(value)
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.closed {
		return written, nil
	}

	for len(value) > 0 {
		newline := bytes.IndexByte(value, '\n')
		segment := value
		complete := false
		if newline >= 0 {
			segment = value[:newline]
			complete = true
		}
		stream.appendSegmentLocked(segment)
		if complete {
			stream.publishPartialLocked()
			value = value[newline+1:]
			continue
		}
		break
	}
	return written, nil
}

func (stream *LogStream) appendSegmentLocked(segment []byte) {
	if stream.truncated || len(segment) == 0 {
		return
	}
	remaining := stream.maxLineBytes - len(stream.partial)
	if remaining <= 0 {
		stream.truncated = true
		return
	}
	if len(segment) <= remaining {
		stream.partial = append(stream.partial, segment...)
		return
	}
	stream.partial = append(stream.partial, segment[:remaining]...)
	stream.truncated = true
}

func (stream *LogStream) publishPartialLocked() {
	line := string(stream.partial)
	if stream.truncated {
		line += "... [truncated]"
	}
	stream.publishLocked(line + "\n")
	stream.partial = stream.partial[:0]
	stream.truncated = false
}

func (stream *LogStream) publishLocked(line string) {
	for len(stream.lines) > 0 && (len(stream.lines) >= stream.capacity || stream.retainedBytes+len(line) > stream.maxBytes) {
		stream.retainedBytes -= len(stream.lines[0])
		copy(stream.lines, stream.lines[1:])
		stream.lines = stream.lines[:len(stream.lines)-1]
	}
	if len(line) <= stream.maxBytes {
		stream.lines = append(stream.lines, line)
		stream.retainedBytes += len(line)
	}
	for _, output := range stream.subscribers {
		select {
		case output <- line:
		default:
			// Keep each subscription bounded and biased toward the newest
			// records, just like the retained history.
			select {
			case <-output:
			default:
			}
			select {
			case output <- line:
			default:
			}
		}
	}
}

// Snapshot returns a copy of the currently retained lines, oldest first.
func (stream *LogStream) Snapshot() []string {
	if stream == nil {
		return nil
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return append([]string(nil), stream.lines...)
}

// Subscribe returns retained history followed by live records. Closing or
// canceling a subscription closes its Lines channel.
func (stream *LogStream) Subscribe() *LogSubscription {
	if stream == nil {
		closed := make(chan string)
		close(closed)
		return &LogSubscription{lines: closed}
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	bufferSize := stream.capacity + stream.subscriberBuffer
	if bufferSize < 1 {
		bufferSize = 1
	}
	output := make(chan string, bufferSize)
	for _, line := range stream.lines {
		output <- line
	}
	if stream.closed {
		close(output)
		return &LogSubscription{lines: output}
	}
	stream.nextSubscriberID++
	id := stream.nextSubscriberID
	stream.subscribers[id] = output
	return &LogSubscription{stream: stream, id: id, lines: output}
}

// Close flushes a partial line and closes all live subscriptions. It is safe
// to call more than once.
func (stream *LogStream) Close() error {
	if stream == nil {
		return nil
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.closed {
		return nil
	}
	if len(stream.partial) > 0 || stream.truncated {
		stream.publishPartialLocked()
	}
	stream.closed = true
	for id, output := range stream.subscribers {
		delete(stream.subscribers, id)
		close(output)
	}
	return nil
}

// LogSubscription is a retained-plus-live stream of text log lines.
type LogSubscription struct {
	stream *LogStream
	id     uint64
	lines  <-chan string
	once   sync.Once
}

// fanoutHandler sends each slog record to the instance's bounded text stream
// and, when configured, the daemon's normal logger.
type fanoutHandler struct {
	handlers []slog.Handler
}

func (handler fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, destination := range handler.handlers {
		if destination.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (handler fanoutHandler) Handle(ctx context.Context, record slog.Record) error {
	var combined error
	for _, destination := range handler.handlers {
		if destination.Enabled(ctx, record.Level) {
			combined = errors.Join(combined, destination.Handle(ctx, record.Clone()))
		}
	}
	return combined
}

func (handler fanoutHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	cloned := make([]slog.Handler, len(handler.handlers))
	for index, destination := range handler.handlers {
		cloned[index] = destination.WithAttrs(attributes)
	}
	return fanoutHandler{handlers: cloned}
}

func (handler fanoutHandler) WithGroup(name string) slog.Handler {
	cloned := make([]slog.Handler, len(handler.handlers))
	for index, destination := range handler.handlers {
		cloned[index] = destination.WithGroup(name)
	}
	return fanoutHandler{handlers: cloned}
}

// Lines returns the channel used to consume log lines.
func (subscription *LogSubscription) Lines() <-chan string {
	if subscription == nil {
		return nil
	}
	return subscription.lines
}

// Cancel unsubscribes and closes Lines. It is safe to call more than once.
func (subscription *LogSubscription) Cancel() {
	if subscription == nil {
		return
	}
	subscription.once.Do(func() {
		if subscription.stream == nil {
			return
		}
		stream := subscription.stream
		stream.mu.Lock()
		defer stream.mu.Unlock()
		output, exists := stream.subscribers[subscription.id]
		if !exists {
			return
		}
		delete(stream.subscribers, subscription.id)
		close(output)
	})
}
