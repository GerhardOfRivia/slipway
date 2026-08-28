package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadResolvesPathsRelativeToConfigFile(t *testing.T) {
	root := t.TempDir()
	configDirectory := filepath.Join(root, "configuration")
	if err := os.Mkdir(configDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(configDirectory, "slipway.yaml")
	contents := `
database:
  path: state/../queue/slipway.db
watches:
  - name: incoming
    path: ../incoming/./nested
    pipeline:
      - name: path-program
        program: ./bin/process
        working_directory: work/{{stem}}
      - name: nested-program
        program: tools/process
        working_directory: stage/../ready
      - name: path-lookup
        program: process-file
        working_directory: "{{dir}}/work/{{stem}}"
      - name: file-directory
        program: process-other
        working_directory: "{{file}}/.."
`
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	otherDirectory := filepath.Join(root, "other-cwd")
	if err := os.Mkdir(otherDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	originalDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(otherDirectory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalDirectory); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	wantDatabase, err := CanonicalDatabasePath(filepath.Join(configDirectory, "queue", "slipway.db"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Path != wantDatabase {
		t.Errorf("database path = %q, want %q", cfg.Database.Path, wantDatabase)
	}
	if want := filepath.Join(root, "incoming", "nested"); cfg.Watches[0].Path != want {
		t.Errorf("watch path = %q, want %q", cfg.Watches[0].Path, want)
	}

	commands := cfg.Watches[0].Pipeline
	if want := filepath.Join(configDirectory, "bin", "process"); commands[0].Program != want {
		t.Errorf("relative program = %q, want %q", commands[0].Program, want)
	}
	if want := filepath.Join(configDirectory, "work", "{{stem}}"); commands[0].WorkingDir != want {
		t.Errorf("relative template working directory = %q, want %q", commands[0].WorkingDir, want)
	}
	if want := filepath.Join(configDirectory, "tools", "process"); commands[1].Program != want {
		t.Errorf("nested program = %q, want %q", commands[1].Program, want)
	}
	if want := filepath.Join(configDirectory, "ready"); commands[1].WorkingDir != want {
		t.Errorf("static working directory = %q, want %q", commands[1].WorkingDir, want)
	}
	if commands[2].Program != "process-file" {
		t.Errorf("bare program = %q, want PATH lookup name", commands[2].Program)
	}
	if commands[2].WorkingDir != "{{dir}}/work/{{stem}}" {
		t.Errorf("dir-template working directory = %q", commands[2].WorkingDir)
	}
	if commands[3].Program != "process-other" {
		t.Errorf("second bare program = %q, want PATH lookup name", commands[3].Program)
	}
	if commands[3].WorkingDir != "{{file}}/.." {
		t.Errorf("file-template working directory = %q", commands[3].WorkingDir)
	}
}

func TestLoadCleansAbsoluteProgramAndWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	absoluteProgram := filepath.Join(root, "tools", "..", "bin", "process")
	absoluteWorkingDirectory := filepath.Join(root, "work", "..", "ready")
	filename := filepath.Join(root, "slipway.yaml")
	contents := "watches:\n" +
		"  - name: incoming\n" +
		"    path: ./incoming\n" +
		"    pipeline:\n" +
		"      - name: process\n" +
		"        program: " + absoluteProgram + "\n" +
		"        working_directory: " + absoluteWorkingDirectory + "\n"
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	command := cfg.Watches[0].Pipeline[0]
	if want := filepath.Join(root, "bin", "process"); command.Program != want {
		t.Errorf("program = %q, want %q", command.Program, want)
	}
	if want := filepath.Join(root, "ready"); command.WorkingDir != want {
		t.Errorf("working directory = %q, want %q", command.WorkingDir, want)
	}
}

func TestLoadResolvesStructuredContainerPaths(t *testing.T) {
	directory := t.TempDir()
	filename := filepath.Join(directory, "slipway.yaml")
	contents := `
watches:
  - name: incoming
    path: .
    pipeline:
      - name: dockerized
        executor: docker
        image: registry.example/team/image:latest
        mounts:
          - source: assets/{{stem}}
            target: /assets/{{stem}}
          - source: "{{dir}}"
            target: /input
      - name: local-apptainer
        executor: apptainer
        image: ./images/tool.sif
      - name: remote-apptainer
        executor: apptainer
        image: docker://alpine:latest
      - name: colon-local-apptainer
        executor: apptainer
        image: model:v1.sif
      - name: daemon-apptainer
        executor: apptainer
        image: docker-daemon:alpine:latest
      - name: docker-archive-apptainer
        executor: apptainer
        image: docker-archive:./images/alpine.tar
      - name: oci-archive-apptainer
        executor: apptainer
        image: oci-archive:/var/images/alpine.tar
`
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	commands := cfg.Watches[0].Pipeline
	if commands[0].Image != "registry.example/team/image:latest" {
		t.Errorf("docker image = %q, want unchanged reference", commands[0].Image)
	}
	if want := filepath.Join(directory, "assets", "{{stem}}"); commands[0].Mounts[0].Source != want {
		t.Errorf("static mount source = %q, want %q", commands[0].Mounts[0].Source, want)
	}
	if commands[0].Mounts[0].Target != "/assets/{{stem}}" {
		t.Errorf("mount target = %q, want unchanged container path", commands[0].Mounts[0].Target)
	}
	if commands[0].Mounts[1].Source != "{{dir}}" {
		t.Errorf("job-relative mount source = %q, want unchanged template", commands[0].Mounts[1].Source)
	}
	if want := filepath.Join(directory, "images", "tool.sif"); commands[1].Image != want {
		t.Errorf("local Apptainer image = %q, want %q", commands[1].Image, want)
	}
	if commands[2].Image != "docker://alpine:latest" {
		t.Errorf("remote Apptainer image = %q, want unchanged transport", commands[2].Image)
	}
	if want := filepath.Join(directory, "model:v1.sif"); commands[3].Image != want {
		t.Errorf("colon-bearing local Apptainer image = %q, want %q", commands[3].Image, want)
	}
	if commands[4].Image != "docker-daemon:alpine:latest" {
		t.Errorf("Docker daemon Apptainer image = %q, want unchanged transport", commands[4].Image)
	}
	if want := "docker-archive:" + filepath.Join(directory, "images", "alpine.tar"); commands[5].Image != want {
		t.Errorf("relative Docker archive Apptainer image = %q, want %q", commands[5].Image, want)
	}
	if commands[6].Image != "oci-archive:/var/images/alpine.tar" {
		t.Errorf("absolute OCI archive Apptainer image = %q, want unchanged path", commands[6].Image)
	}
}

func TestCanonicalDatabasePathResolvesExistingSymlinkedParent(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real-data")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedDirectory := filepath.Join(root, "data-link")
	if err := os.Symlink(realDirectory, linkedDirectory); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	got, err := CanonicalDatabasePath(filepath.Join(linkedDirectory, "future", "slipway.db"))
	if err != nil {
		t.Fatalf("CanonicalDatabasePath() error = %v", err)
	}
	want := filepath.Join(realDirectory, "future", "slipway.db")
	if got != want {
		t.Fatalf("CanonicalDatabasePath() = %q, want %q", got, want)
	}
}

func TestCanonicalDatabasePathResolvesExistingTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "actual.db")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "linked.db")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	got, err := CanonicalDatabasePath(link)
	if err != nil {
		t.Fatalf("CanonicalDatabasePath() error = %v", err)
	}
	if got != target {
		t.Fatalf("CanonicalDatabasePath() = %q, want %q", got, target)
	}
}

func TestCanonicalDatabasePathResolvesDanglingFinalSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "future.db")
	link := filepath.Join(root, "linked.db")
	if err := os.Symlink(filepath.Base(target), link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	got, err := CanonicalDatabasePath(link)
	if err != nil {
		t.Fatalf("CanonicalDatabasePath() error = %v", err)
	}
	if got != target {
		t.Fatalf("CanonicalDatabasePath() = %q, want dangling target %q", got, target)
	}
}

func TestCanonicalDatabasePathResolvesChainedDanglingIntermediateSymlinks(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	if err := os.Symlink(filepath.Base(second), first); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(filepath.Join("future", "database"), second); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	got, err := CanonicalDatabasePath(filepath.Join(first, "nested", "slipway.db"))
	if err != nil {
		t.Fatalf("CanonicalDatabasePath() error = %v", err)
	}
	want := filepath.Join(root, "future", "database", "nested", "slipway.db")
	if got != want {
		t.Fatalf("CanonicalDatabasePath() = %q, want chained dangling target %q", got, want)
	}
}

func TestCanonicalDatabasePathRejectsSymlinkLoop(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	if err := os.Symlink(filepath.Base(second), first); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(filepath.Base(first), second); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	_, err := CanonicalDatabasePath(filepath.Join(first, "slipway.db"))
	if err == nil || !strings.Contains(err.Error(), "too many symlinks") {
		t.Fatalf("CanonicalDatabasePath() error = %v, want symlink-depth error", err)
	}
}

func TestPathsEquivalentDetectsHardLinks(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first.db")
	second := filepath.Join(root, "second.db")
	if err := os.WriteFile(first, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(first, second); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	equivalent, err := PathsEquivalent(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent {
		t.Fatalf("PathsEquivalent(%q, %q) = false for hard links", first, second)
	}
}

func TestPathsEquivalentConservativelyReservesMissingCaseAliases(t *testing.T) {
	root := t.TempDir()
	left := filepath.Join(root, "Queue.db")
	right := filepath.Join(root, "queue.db")
	equivalent, err := PathsEquivalent(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent {
		t.Fatal("fresh case aliases were not conservatively reserved")
	}
}

func TestPathsEquivalentReservesMissingUnicodeNormalizationAliases(t *testing.T) {
	root := t.TempDir()
	composed := filepath.Join(root, "caf\u00e9.db")
	decomposed := filepath.Join(root, "cafe\u0301.db")
	equivalent, err := PathsEquivalent(composed, decomposed)
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent {
		t.Fatal("fresh Unicode normalization aliases were not reserved")
	}
}

func TestMissingPathsEquivalentUsesExistingAncestorIdentity(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasDirectory := filepath.Join(root, "alias")
	if err := os.Symlink(realDirectory, aliasDirectory); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	equivalent, err := missingPathsEquivalent(
		filepath.Join(realDirectory, "future", "slipway.db"),
		filepath.Join(aliasDirectory, "future", "slipway.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent {
		t.Fatal("missing paths below the same existing directory were not equivalent")
	}
}

func TestDeepestExistingAncestorAndSuffix(t *testing.T) {
	root := t.TempDir()
	ancestor, suffix, err := deepestExistingAncestorAndSuffix(filepath.Join(root, "future", "nested", "slipway.db"))
	if err != nil {
		t.Fatal(err)
	}
	if ancestor != root {
		t.Fatalf("ancestor = %q, want %q", ancestor, root)
	}
	if want := filepath.Join("future", "nested", "slipway.db"); suffix != want {
		t.Fatalf("suffix = %q, want %q", suffix, want)
	}
}

func TestLoadCanonicalizesDatabaseButNotWatchSymlink(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "linked")
	if err := os.Symlink(realDirectory, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	filename := filepath.Join(root, "slipway.yaml")
	contents := `
database: {path: linked/slipway.db}
watches:
  - name: incoming
    path: linked/incoming
    pipeline: [{name: process, program: process-file}]
`
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := filepath.Join(realDirectory, "slipway.db"); cfg.Database.Path != want {
		t.Errorf("database path = %q, want %q", cfg.Database.Path, want)
	}
	if want := filepath.Join(link, "incoming"); cfg.Watches[0].Path != want {
		t.Errorf("watch path = %q, want lexical path %q", cfg.Watches[0].Path, want)
	}
}
