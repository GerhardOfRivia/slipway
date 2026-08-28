package control

import (
	"strings"
	"testing"
)

func TestStartRequestBoundsConfigurationCount(t *testing.T) {
	t.Parallel()
	paths := make([]string, maxConfigsPerStart+1)
	for index := range paths {
		paths[index] = "/configs/slipway.yaml"
	}
	_, err := (startRequest{ConfigPaths: paths}).paths()
	if err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("paths() error = %v, want configuration-count limit", err)
	}
}
