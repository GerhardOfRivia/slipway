package watcher

import (
	"path"
	"path/filepath"
	"strings"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

// Match reports whether filename is selected by a watch. Patterns without a
// slash are matched against the basename, including for recursively discovered
// files. Patterns with a slash are matched against the slash-separated path
// relative to the watch root. Excludes always take precedence over includes.
// A ** path segment matches zero or more directories.
func Match(watch config.WatchConfig, filename string) bool {
	root, err := filepath.Abs(watch.Path)
	if err != nil {
		return false
	}
	name, err := filepath.Abs(filename)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(root, name)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	if !watch.Recursive && filepath.Dir(relative) != "." {
		return false
	}

	relative = filepath.ToSlash(relative)
	for _, pattern := range watch.Exclude {
		if globMatch(pattern, relative) {
			return false
		}
	}
	if len(watch.Include) == 0 {
		return true
	}
	for _, pattern := range watch.Include {
		if globMatch(pattern, relative) {
			return true
		}
	}
	return false
}

func globMatch(pattern, relative string) bool {
	pattern = strings.TrimPrefix(filepath.ToSlash(pattern), "./")
	if !strings.ContainsRune(pattern, '/') {
		matched, err := path.Match(pattern, path.Base(relative))
		return err == nil && matched
	}
	return matchSegments(strings.Split(pattern, "/"), strings.Split(relative, "/"))
}

func matchSegments(pattern, name []string) bool {
	if len(pattern) == 0 {
		return len(name) == 0
	}
	if pattern[0] == "**" {
		for len(pattern) > 1 && pattern[1] == "**" {
			pattern = pattern[1:]
		}
		if len(pattern) == 1 {
			return true
		}
		for i := 0; i <= len(name); i++ {
			if matchSegments(pattern[1:], name[i:]) {
				return true
			}
		}
		return false
	}
	if len(name) == 0 {
		return false
	}
	matched, err := path.Match(pattern[0], name[0])
	return err == nil && matched && matchSegments(pattern[1:], name[1:])
}
