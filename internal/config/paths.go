package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const (
	fileTemplate                 = "{{file}}"
	dirTemplate                  = "{{dir}}"
	maxCanonicalPathSymlinkDepth = 255
)

func resolveConfiguredPaths(cfg *Config, filename string) error {
	configFile, err := filepath.Abs(filename)
	if err != nil {
		return fmt.Errorf("resolve config file path: %w", err)
	}
	configDirectory := filepath.Dir(filepath.Clean(configFile))

	databasePath := resolveRelativePath(configDirectory, cfg.Database.Path)
	cfg.Database.Path, err = CanonicalDatabasePath(databasePath)
	if err != nil {
		return fmt.Errorf("resolve database.path: %w", err)
	}

	for watchIndex := range cfg.Watches {
		watch := &cfg.Watches[watchIndex]
		watch.Path = resolveRelativePath(configDirectory, watch.Path)
		for commandIndex := range watch.Pipeline {
			command := &watch.Pipeline[commandIndex]
			command.Program = resolveProgram(configDirectory, command.Program)
			command.WorkingDir = resolveWorkingDirectory(configDirectory, command.WorkingDir)
			if command.Executor == ExecutorApptainer && command.usesStructuredContainer() {
				command.Image = resolveApptainerImage(configDirectory, command.Image)
			}
			for mountIndex := range command.Mounts {
				mount := &command.Mounts[mountIndex]
				mount.Source = resolveTemplatePath(configDirectory, mount.Source)
			}
		}
	}
	return nil
}

func resolveRelativePath(base, name string) string {
	if filepath.IsAbs(name) {
		return filepath.Clean(name)
	}
	return filepath.Clean(filepath.Join(base, name))
}

// resolveProgram leaves bare executable names for PATH lookup. Programs that
// contain a path separator are paths, so relative ones are anchored beside the
// YAML file and absolute ones are cleaned.
func resolveProgram(configDirectory, program string) string {
	if filepath.IsAbs(program) {
		return filepath.Clean(program)
	}
	if strings.ContainsAny(program, `/\`) {
		return resolveRelativePath(configDirectory, program)
	}
	return program
}

func resolveWorkingDirectory(configDirectory, workingDirectory string) string {
	if workingDirectory == "" {
		return ""
	}
	return resolveTemplatePath(configDirectory, workingDirectory)
}

// resolveTemplatePath anchors static and job-relative template paths beside
// the YAML file. {{file}} and {{dir}} already expand to absolute paths, so
// prefixing either would produce an invalid combined path.
func resolveTemplatePath(configDirectory, name string) string {
	// {{file}} and {{dir}} expand to absolute paths. Prefixing either with the
	// config directory would keep the expanded result underneath that prefix.
	if strings.Contains(name, fileTemplate) || strings.Contains(name, dirTemplate) {
		return name
	}
	return resolveRelativePath(configDirectory, name)
}

func resolveApptainerImage(configDirectory, image string) string {
	if image == "" || strings.Contains(image, "://") {
		return image
	}
	if strings.HasPrefix(image, "docker-daemon:") {
		return image
	}
	for _, transport := range []string{"docker-archive:", "oci-archive:"} {
		if strings.HasPrefix(image, transport) {
			archive := strings.TrimPrefix(image, transport)
			if archive == "" {
				return image
			}
			return transport + resolveTemplatePath(configDirectory, archive)
		}
	}
	return resolveTemplatePath(configDirectory, image)
}

// CanonicalDatabasePath returns a clean absolute database path with every
// inspectable symlink resolved, including a symlink whose target does not exist
// yet. The database and trailing directories may also be absent. This makes
// paths suitable for duplicate-database checks before SQLite creates the file.
func CanonicalDatabasePath(name string) (string, error) {
	if name == "" {
		return "", errors.New("database path is empty")
	}
	absolute, err := filepath.Abs(name)
	if err != nil {
		return "", fmt.Errorf("make database path absolute: %w", err)
	}
	absolute = filepath.Clean(absolute)

	current := absolute
	symlinksFollowed := 0
	for {
		root, components := splitAbsolutePath(current)
		resolved := root
		restart := false
		for index, component := range components {
			candidate := filepath.Join(resolved, component)
			info, inspectErr := os.Lstat(candidate)
			if errors.Is(inspectErr, os.ErrNotExist) {
				return joinPathComponents(candidate, components[index+1:]), nil
			}
			if inspectErr != nil {
				return "", fmt.Errorf("inspect database path prefix %s: %w", candidate, inspectErr)
			}
			if info.Mode()&os.ModeSymlink == 0 {
				resolved = candidate
				continue
			}

			symlinksFollowed++
			if symlinksFollowed > maxCanonicalPathSymlinkDepth {
				return "", fmt.Errorf("resolve database path %s: too many symlinks", absolute)
			}
			target, readErr := os.Readlink(candidate)
			if readErr != nil {
				return "", fmt.Errorf("read database path symlink %s: %w", candidate, readErr)
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(candidate), target)
			}
			current = joinPathComponents(target, components[index+1:])
			restart = true
			break
		}
		if !restart {
			return filepath.Clean(resolved), nil
		}
	}
}

// PathsEquivalent reports whether two paths address the same filesystem
// object, or—when both final paths are absent—potentially equivalent unresolved
// names beneath equivalent existing directories. The latter conservatively
// reserves Unicode-normalized and case-folded aliases as well as paths reached
// through bind mounts and firmlinks before SQLite creates the database file.
// The comparison is read-only.
func PathsEquivalent(left, right string) (bool, error) {
	left, err := CanonicalDatabasePath(left)
	if err != nil {
		return false, err
	}
	right, err = CanonicalDatabasePath(right)
	if err != nil {
		return false, err
	}
	if left == right {
		return true, nil
	}
	leftInfo, leftExists, err := statComparablePath(left)
	if err != nil {
		return false, err
	}
	rightInfo, rightExists, err := statComparablePath(right)
	if err != nil {
		return false, err
	}
	// A daemon-owned database can appear between the two stats. Repeat an
	// asymmetric snapshot once so an alias does not slip through during that
	// creation transition.
	if leftExists != rightExists {
		leftInfo, leftExists, err = statComparablePath(left)
		if err != nil {
			return false, err
		}
		rightInfo, rightExists, err = statComparablePath(right)
		if err != nil {
			return false, err
		}
	}
	if leftExists && rightExists {
		return os.SameFile(leftInfo, rightInfo), nil
	}
	if leftExists != rightExists {
		return false, nil
	}
	return missingPathsEquivalent(left, right)
}

func missingPathsEquivalent(left, right string) (bool, error) {
	leftAncestor, leftSuffix, err := deepestExistingAncestorAndSuffix(left)
	if err != nil {
		return false, err
	}
	rightAncestor, rightSuffix, err := deepestExistingAncestorAndSuffix(right)
	if err != nil {
		return false, err
	}
	leftAncestorInfo, err := os.Stat(leftAncestor)
	if err != nil {
		return false, fmt.Errorf("inspect path ancestor %s: %w", leftAncestor, err)
	}
	rightAncestorInfo, err := os.Stat(rightAncestor)
	if err != nil {
		return false, fmt.Errorf("inspect path ancestor %s: %w", rightAncestor, err)
	}
	if !os.SameFile(leftAncestorInfo, rightAncestorInfo) {
		return false, nil
	}
	return unresolvedSuffixesEquivalent(leftSuffix, rightSuffix), nil
}

func unresolvedSuffixesEquivalent(left, right string) bool {
	if left == right {
		return true
	}
	if !utf8.ValidString(left) || !utf8.ValidString(right) {
		return false
	}
	left = norm.NFC.String(left)
	right = norm.NFC.String(right)
	return left == right || strings.EqualFold(left, right)
}

func statComparablePath(path string) (os.FileInfo, bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inspect path %s: %w", path, err)
	}
	return info, true, nil
}

func deepestExistingAncestorAndSuffix(path string) (string, string, error) {
	current := filepath.Clean(path)
	missing := make([]string, 0, 4)
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return "", "", fmt.Errorf("path ancestor %s is not a directory", current)
			}
			suffix := missing[len(missing)-1]
			for index := len(missing) - 2; index >= 0; index-- {
				suffix = filepath.Join(suffix, missing[index])
			}
			return current, suffix, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", "", fmt.Errorf("inspect path ancestor %s: %w", current, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", "", fmt.Errorf("no existing ancestor for %s", path)
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func splitAbsolutePath(path string) (string, []string) {
	volume := filepath.VolumeName(path)
	root := volume + string(filepath.Separator)
	remainder := strings.TrimPrefix(path[len(volume):], string(filepath.Separator))
	if remainder == "" {
		return root, nil
	}
	return root, strings.Split(remainder, string(filepath.Separator))
}

func joinPathComponents(base string, components []string) string {
	for _, component := range components {
		base = filepath.Join(base, component)
	}
	return filepath.Clean(base)
}
