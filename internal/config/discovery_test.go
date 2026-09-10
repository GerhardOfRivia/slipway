package config

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDiscoverExplicitFileRegardlessOfExtension(t *testing.T) {
	root := t.TempDir()
	filename := filepath.Join(root, "onderzeeer.conf")
	writeDiscoveryFile(t, filename)

	paths, err := Discover(filepath.Join(root, ".", "subdir", "..", "onderzeeer.conf"))
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	want := []string{filename}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("Discover() = %q, want %q", paths, want)
	}
}

func TestDiscoverExplicitRegularFileSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.data")
	link := filepath.Join(root, "config-link")
	writeDiscoveryFile(t, target)
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	paths, err := Discover(link)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if want := []string{link}; !reflect.DeepEqual(paths, want) {
		t.Fatalf("Discover() = %q, want symlink path %q", paths, want)
	}
}

func TestDiscoverExplicitDirectorySortedAndNonRecursive(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"b.yml", "a.yaml", "ignored.txt", "UPPER.YAML"} {
		writeDiscoveryFile(t, filepath.Join(root, name))
	}
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	writeDiscoveryFile(t, filepath.Join(nested, "nested.yaml"))
	target := filepath.Join(root, "ignored.txt")
	link := filepath.Join(root, "c.yaml")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	paths, err := Discover(root)
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	want := []string{
		filepath.Join(root, "a.yaml"),
		filepath.Join(root, "b.yml"),
		link,
	}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("Discover() = %q, want %q", paths, want)
	}
}

func TestDiscoverExplicitDirectoryMustContainYAML(t *testing.T) {
	root := t.TempDir()
	writeDiscoveryFile(t, filepath.Join(root, "not-yaml.txt"))

	_, err := Discover(root)
	if err == nil || !strings.Contains(err.Error(), "contains no YAML files") || !strings.Contains(err.Error(), root) {
		t.Fatalf("Discover() error = %v", err)
	}
}

func TestDiscoverExplicitMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	_, err := Discover(missing)
	if err == nil || !strings.Contains(err.Error(), "explicit configuration") || !strings.Contains(err.Error(), missing) {
		t.Fatalf("Discover() error = %v", err)
	}
}

func TestDiscoverExplicitRejectsNonFile(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "config.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Skipf("Unix sockets unavailable: %v", err)
	}
	defer listener.Close()

	_, err = Discover(socket)
	if err == nil || !strings.Contains(err.Error(), "neither a regular file nor a directory") {
		t.Fatalf("Discover() error = %v", err)
	}
}

func TestDiscoverRequiresExplicitPath(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	writeDiscoveryFile(t, "onderzeeer")
	writeDiscoveryFile(t, "onderzeeer.yaml")
	for _, selection := range []string{"", " ", "\t\n"} {
		_, err := Discover(selection)
		if err == nil || err.Error() != "configuration path is required" {
			t.Errorf("Discover(%q) error = %v, want required path error", selection, err)
		}
	}
}

func writeDiscoveryFile(t *testing.T, filename string) {
	t.Helper()
	if err := os.WriteFile(filename, []byte("watches: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}
