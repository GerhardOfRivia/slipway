package watcher

import (
	"path/filepath"
	"testing"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

func TestMatch(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name      string
		watch     config.WatchConfig
		path      string
		wantMatch bool
	}{
		{
			name:      "recursive basename pattern",
			watch:     config.WatchConfig{Path: root, Recursive: true, Include: []string{"*.csv"}},
			path:      filepath.Join(root, "year", "report.csv"),
			wantMatch: true,
		},
		{
			name:      "relative pattern",
			watch:     config.WatchConfig{Path: root, Recursive: true, Include: []string{"reports/*.csv"}},
			path:      filepath.Join(root, "reports", "report.csv"),
			wantMatch: true,
		},
		{
			name:      "double star",
			watch:     config.WatchConfig{Path: root, Recursive: true, Include: []string{"reports/**/final-*.csv"}},
			path:      filepath.Join(root, "reports", "2026", "08", "final-a.csv"),
			wantMatch: true,
		},
		{
			name:      "double star matches no directory",
			watch:     config.WatchConfig{Path: root, Recursive: true, Include: []string{"**/*.csv"}},
			path:      filepath.Join(root, "report.csv"),
			wantMatch: true,
		},
		{
			name:      "exclude wins",
			watch:     config.WatchConfig{Path: root, Recursive: true, Include: []string{"*.csv"}, Exclude: []string{"private/*.csv"}},
			path:      filepath.Join(root, "private", "report.csv"),
			wantMatch: false,
		},
		{
			name:      "empty include matches",
			watch:     config.WatchConfig{Path: root, Recursive: true},
			path:      filepath.Join(root, "any.dat"),
			wantMatch: true,
		},
		{
			name:      "non-recursive rejects nested",
			watch:     config.WatchConfig{Path: root, Include: []string{"*.csv"}},
			path:      filepath.Join(root, "nested", "report.csv"),
			wantMatch: false,
		},
		{
			name:      "outside root",
			watch:     config.WatchConfig{Path: root, Recursive: true},
			path:      filepath.Join(filepath.Dir(root), "outside.csv"),
			wantMatch: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Match(test.watch, test.path); got != test.wantMatch {
				t.Fatalf("Match() = %v, want %v", got, test.wantMatch)
			}
		})
	}
}
