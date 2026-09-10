package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Discover resolves an explicitly selected configuration file or a non-recursive
// directory of YAML files. A path is required; no default locations are searched.
func Discover(explicit string) ([]string, error) {
	if strings.TrimSpace(explicit) == "" {
		return nil, errors.New("configuration path is required")
	}

	absolute, err := absolutePath(explicit)
	if err != nil {
		return nil, fmt.Errorf("resolve explicit configuration %q: %w", explicit, err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return nil, fmt.Errorf("inspect explicit configuration %s: %w", absolute, err)
	}
	if info.Mode().IsRegular() {
		return []string{absolute}, nil
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("explicit configuration %s is neither a regular file nor a directory", absolute)
	}

	files, err := yamlFilesIn(absolute)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("explicit configuration directory contains no YAML files: %s", absolute)
	}
	return files, nil
}

func yamlFilesIn(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read configuration directory %s: %w", directory, err)
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		extension := filepath.Ext(entry.Name())
		if extension != ".yaml" && extension != ".yml" {
			continue
		}

		filename := filepath.Join(directory, entry.Name())
		info, err := os.Stat(filename)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect configuration candidate %s: %w", filename, err)
		}
		if info.Mode().IsRegular() {
			files = append(files, filepath.Clean(filename))
		}
	}
	sort.Strings(files)
	return files, nil
}

func absolutePath(name string) (string, error) {
	absolute, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}
