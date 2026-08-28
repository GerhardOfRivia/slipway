package control

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLogStreamRetainsBoundedCompleteLines(t *testing.T) {
	t.Parallel()
	stream := NewLogStream(2, 1, 1024)
	if written, err := stream.Write([]byte("one\ntwo")); err != nil || written != len("one\ntwo") {
		t.Fatalf("first Write = %d, %v", written, err)
	}
	if _, err := stream.Write([]byte(" continued\nthree\n")); err != nil {
		t.Fatal(err)
	}
	if got, want := stream.Snapshot(), []string{"two continued\n", "three\n"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot = %#v, want %#v", got, want)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("ignored\n")); err != nil {
		t.Fatal(err)
	}
	if got, want := stream.Snapshot(), []string{"two continued\n", "three\n"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot after Close = %#v, want %#v", got, want)
	}
}

func TestLogStreamRetainsWithinByteBudget(t *testing.T) {
	t.Parallel()
	stream := newLogStream(10, 1, 100, 8)
	if _, err := stream.Write([]byte("one\ntwo\nthree\n")); err != nil {
		t.Fatal(err)
	}
	if got, want := stream.Snapshot(), []string{"three\n"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("byte-bounded snapshot = %#v, want %#v", got, want)
	}
	if _, err := stream.Write([]byte("12345678\n")); err != nil {
		t.Fatal(err)
	}
	if got := stream.Snapshot(); len(got) != 0 {
		t.Fatalf("line larger than retention budget was retained: %#v", got)
	}
}

func TestLogStreamFlushesAndTruncatesPartialLine(t *testing.T) {
	t.Parallel()
	stream := NewLogStream(4, 1, 5)
	_, _ = stream.Write([]byte("123456789"))
	if got := stream.Snapshot(); len(got) != 0 {
		t.Fatalf("incomplete line was published early: %#v", got)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	lines := stream.Snapshot()
	if len(lines) != 1 || lines[0] != "12345... [truncated]\n" {
		t.Fatalf("truncated line = %#v", lines)
	}
}

func TestLogSubscriptionReplaysBroadcastsAndCancels(t *testing.T) {
	t.Parallel()
	stream := NewLogStream(2, 1, 1024)
	_, _ = stream.Write([]byte("retained\n"))
	subscription := stream.Subscribe()
	if got := receiveLine(t, subscription.Lines()); got != "retained\n" {
		t.Fatalf("replayed line = %q", got)
	}
	_, _ = stream.Write([]byte("live\n"))
	if got := receiveLine(t, subscription.Lines()); got != "live\n" {
		t.Fatalf("live line = %q", got)
	}
	subscription.Cancel()
	if _, open := <-subscription.Lines(); open {
		t.Fatal("subscription channel remained open after Cancel")
	}
	subscription.Cancel()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogSubscriptionCannotBlockOrGrowWithoutBound(t *testing.T) {
	t.Parallel()
	stream := NewLogStream(1, 1, 1024)
	subscription := stream.Subscribe()
	defer subscription.Cancel()
	for index := 0; index < 100; index++ {
		_, _ = stream.Write([]byte(strings.Repeat("x", index%5) + "\n"))
	}
	if got, limit := len(subscription.Lines()), cap(subscription.lines); got > limit {
		t.Fatalf("subscription buffered %d lines, capacity %d", got, limit)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	for range subscription.Lines() {
	}
}

func TestSubscribeAfterCloseReplaysThenCloses(t *testing.T) {
	t.Parallel()
	stream := NewLogStream(2, 1, 1024)
	_, _ = stream.Write([]byte("done\n"))
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	subscription := stream.Subscribe()
	defer subscription.Cancel()
	if got := receiveLine(t, subscription.Lines()); got != "done\n" {
		t.Fatalf("replayed line = %q", got)
	}
	if _, open := <-subscription.Lines(); open {
		t.Fatal("completed stream subscription remained open")
	}
}

func receiveLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	select {
	case line, open := <-lines:
		if !open {
			t.Fatal("log channel closed before a line arrived")
		}
		return line
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for log line")
		return ""
	}
}
