package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const systemConfigDirectory = "/etc/slipway.d"

type discoveryLocations struct {
	directories []string
	fallback    string
}

// Discover resolves the configuration files slipway should load. An explicit
// path may identify either one file or a non-recursive directory of YAML
// files. Without an explicit path, system and user configuration directories
// are combined; ./slipway.yaml is used only when neither directory yields files.
func Discover(explicit string) ([]string, error) {
	if explicit != "" {
		return discoverWithLocations(explicit, discoveryLocations{})
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("find user configuration directory: %w", err)
	}
	return discoverWithLocations("", discoveryLocations{
		directories: []string{
			systemConfigDirectory,
			filepath.Join(home, ".local", "slipway.d"),
		},
		fallback: filepath.Join(".", "slipway.yaml"),
	})
}

// discoverWithLocations keeps default location selection separate from the
// filesystem rules, allowing tests to use isolated temporary directories.
func discoverWithLocations(explicit string, locations discoveryLocations) ([]string, error) {
	if explicit != "" {
		return discoverExplicit(explicit)
	}

	directories := make([]string, 0, len(locations.directories))
	files := make([]string, 0)
	for _, directory := range locations.directories {
		absolute, err := absolutePath(directory)
		if err != nil {
			return nil, fmt.Errorf("resolve configuration directory %q: %w", directory, err)
		}
		directories = append(directories, absolute)

		info, err := os.Stat(absolute)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect configuration directory %s: %w", absolute, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("configuration location %s is not a directory", absolute)
		}

		discovered, err := yamlFilesIn(absolute)
		if err != nil {
			return nil, err
		}
		files = append(files, discovered...)
	}
	if len(files) > 0 {
		return files, nil
	}

	if locations.fallback == "" {
		return nil, noConfigurationsError(directories)
	}
	fallback, err := absolutePath(locations.fallback)
	if err != nil {
		return nil, fmt.Errorf("resolve fallback configuration %q: %w", locations.fallback, err)
	}
	searched := append(directories, fallback)
	info, err := os.Stat(fallback)
	if errors.Is(err, os.ErrNotExist) {
		return nil, noConfigurationsError(searched)
	}
	if err != nil {
		return nil, fmt.Errorf("inspect fallback configuration %s: %w", fallback, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("fallback configuration %s is not a regular file; searched: %s", fallback, formatLocations(searched))
	}
	return []string{fallback}, nil
}

func discoverExplicit(explicit string) ([]string, error) {
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

func noConfigurationsError(locations []string) error {
	return fmt.Errorf("no configuration files found; searched: %s", formatLocations(locations))
}

func formatLocations(locations []string) string {
	quoted := make([]string, len(locations))
	for i, location := range locations {
		quoted[i] = fmt.Sprintf("%q", location)
	}
	return strings.Join(quoted, ", ")
}
