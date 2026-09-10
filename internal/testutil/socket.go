// Package testutil provides shared helpers for tests.
package testutil

import (
	"os"
	"testing"
)

// SocketDir creates a private temporary directory for Unix sockets and removes
// it after the test. Use /tmp and a short prefix so neither long test names nor
// TMPDIR/GOTMPDIR can exhaust the Unix socket pathname limit (107 bytes on Linux).
func SocketDir(t testing.TB) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "onderzeeer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("remove socket directory: %v", err)
		}
	})
	return directory
}
