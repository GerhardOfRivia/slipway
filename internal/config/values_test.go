package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadExpandsReusableValuesAcrossPipelineFields(t *testing.T) {
	sharedRoot := filepath.Join(t.TempDir(), "shared root")
	filename := writeConfig(t, fmt.Sprintf(`
values:
  shared_root: %q
  image_repo: registry.example/tools
  image_tag: "1.2"
  container_root: /payload
  container_command: analyze
  mount_label: shared
watches:
  - name: incoming
    path: .
    pipeline:
      - name: host
        program: process
        args:
          - "{{shared_root}}/input"
          - "--name={{basename}}"
        working_directory: "{{shared_root}}/work"
        output: "{{shared_root}}/{{stem}}.out"
        env:
          SHARED_ROOT: "{{shared_root}}"
      - name: container
        executor: docker
        image: "{{image_repo}}/processor:{{image_tag}}"
        container_args:
          - "--label=shared={{shared_root}}"
        mounts:
          - source: "{{shared_root}}/source"
            target: "{{container_root}}/input"
            options:
              - ro
              - "relabel={{mount_label}}"
        container_env:
          SHARED_ROOT: "{{shared_root}}"
        command: "{{container_command}}"
        command_args:
          - "{{container_root}}/{{basename}}"
        working_directory: "{{shared_root}}/runtime"
        output: "{{shared_root}}/{{stem}}.json"
        env:
          HOST_ROOT: "{{shared_root}}"
`, sharedRoot))

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.Values["shared_root"]; got != sharedRoot {
		t.Fatalf("values.shared_root = %q, want %q", got, sharedRoot)
	}

	host := cfg.Watches[0].Pipeline[0]
	if want := []string{filepath.Join(sharedRoot, "input"), "--name={{basename}}"}; !reflect.DeepEqual(host.Args, want) {
		t.Errorf("host args = %#v, want %#v", host.Args, want)
	}
	if want := filepath.Join(sharedRoot, "work"); host.WorkingDir != want {
		t.Errorf("host working directory = %q, want %q", host.WorkingDir, want)
	}
	if want := filepath.Join(sharedRoot, "{{stem}}.out"); host.Output != want {
		t.Errorf("host output = %q, want %q", host.Output, want)
	}
	if host.Env["SHARED_ROOT"] != sharedRoot {
		t.Errorf("host environment = %#v, want SHARED_ROOT=%q", host.Env, sharedRoot)
	}

	container := cfg.Watches[0].Pipeline[1]
	if container.Image != "registry.example/tools/processor:1.2" {
		t.Errorf("container image = %q", container.Image)
	}
	if want := []string{"--label=shared=" + sharedRoot}; !reflect.DeepEqual(container.ContainerArgs, want) {
		t.Errorf("container args = %#v, want %#v", container.ContainerArgs, want)
	}
	wantMounts := []MountConfig{{
		Source:  filepath.Join(sharedRoot, "source"),
		Target:  "/payload/input",
		Options: []string{"ro", "relabel=shared"},
	}}
	if !reflect.DeepEqual(container.Mounts, wantMounts) {
		t.Errorf("container mounts = %#v, want %#v", container.Mounts, wantMounts)
	}
	if container.ContainerEnv["SHARED_ROOT"] != sharedRoot {
		t.Errorf("container environment = %#v, want SHARED_ROOT=%q", container.ContainerEnv, sharedRoot)
	}
	if container.Command != "analyze" {
		t.Errorf("container command = %q, want analyze", container.Command)
	}
	if want := []string{"/payload/{{basename}}"}; !reflect.DeepEqual(container.CommandArgs, want) {
		t.Errorf("container command args = %#v, want %#v", container.CommandArgs, want)
	}
	if want := filepath.Join(sharedRoot, "runtime"); container.WorkingDir != want {
		t.Errorf("container working directory = %q, want %q", container.WorkingDir, want)
	}
	if want := filepath.Join(sharedRoot, "{{stem}}.json"); container.Output != want {
		t.Errorf("container output = %q, want %q", container.Output, want)
	}
	if container.Env["HOST_ROOT"] != sharedRoot {
		t.Errorf("container host environment = %#v, want HOST_ROOT=%q", container.Env, sharedRoot)
	}
}

func TestLoadResolvesNestedValuesAndPreservesOtherTemplates(t *testing.T) {
	filename := writeConfig(t, `
values:
  base: /srv
  shared: "{{base}}/shared"
  per_job: "{{shared}}/{{basename}}"
  downstream: "{{third_party}}"
  empty: ""
watches:
  - name: incoming
    path: .
    pipeline:
      - name: inspect
        program: inspect
        args:
          - "{{per_job}}"
          - "{{downstream}}"
          - "{{unknown}}"
          - "{{base}}{{base}}"
          - "before{{empty}}after"
          - "{{empty}}"
`)

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	wantValues := map[string]string{
		"base":       "/srv",
		"shared":     "/srv/shared",
		"per_job":    "/srv/shared/{{basename}}",
		"downstream": "{{third_party}}",
		"empty":      "",
	}
	if !reflect.DeepEqual(cfg.Values, wantValues) {
		t.Errorf("resolved values = %#v, want %#v", cfg.Values, wantValues)
	}
	wantArgs := []string{
		"/srv/shared/{{basename}}",
		"{{third_party}}",
		"{{unknown}}",
		"/srv/srv",
		"beforeafter",
		"",
	}
	if got := cfg.Watches[0].Pipeline[0].Args; !reflect.DeepEqual(got, wantArgs) {
		t.Fatalf("args = %#v, want %#v", got, wantArgs)
	}
}

func TestLoadRejectsInvalidReusableValues(t *testing.T) {
	tests := []struct {
		name       string
		valuesYAML string
		wantError  string
	}{
		{name: "empty key", valuesYAML: `  "": value`, wantError: "invalid key"},
		{name: "leading digit", valuesYAML: `  9lives: value`, wantError: "invalid key"},
		{name: "hyphen", valuesYAML: `  shared-root: value`, wantError: "invalid key"},
		{name: "space", valuesYAML: `  "shared root": value`, wantError: "invalid key"},
		{name: "non scalar", valuesYAML: `  shared: [one, two]`, wantError: "decode config"},
		{name: "self cycle", valuesYAML: `  shared: "{{shared}}"`, wantError: "shared -> shared"},
		{name: "multi value cycle", valuesYAML: "  first: \"{{second}}\"\n  second: \"{{first}}\"", wantError: "first -> second -> first"},
	}
	for _, reserved := range []string{"file", "dir", "basename", "stem", "ext", "job_id"} {
		tests = append(tests, struct {
			name       string
			valuesYAML string
			wantError  string
		}{
			name:       "reserved " + reserved,
			valuesYAML: "  " + reserved + ": value",
			wantError:  "reserved",
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filename := writeConfig(t, "values:\n"+test.valuesYAML+`
watches:
  - name: incoming
    path: .
    pipeline:
      - name: inspect
        program: inspect
`)
			_, err := Load(filename)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Load() error = %v, want error containing %q", err, test.wantError)
			}
		})
	}
}

func TestLoadResolvesPathsAfterReusableValueExpansion(t *testing.T) {
	configDirectory := t.TempDir()
	absoluteRoot := filepath.Join(t.TempDir(), "absolute-root")
	filename := filepath.Join(configDirectory, "slipway.yaml")
	contents := fmt.Sprintf(`
values:
  absolute_root: %q
  relative_root: relative-root
  job_root: "{{dir}}/job-root"
  target_root: /container
watches:
  - name: incoming
    path: .
    pipeline:
      - name: absolute
        executor: docker
        image: example/image
        working_directory: "{{absolute_root}}/work"
        mounts:
          - source: "{{absolute_root}}/input"
            target: "{{target_root}}/input"
      - name: relative
        executor: docker
        image: example/image
        working_directory: "{{relative_root}}/work"
        mounts:
          - source: "{{relative_root}}/input"
            target: /input
      - name: per-job
        executor: docker
        image: example/image
        working_directory: "{{job_root}}/work"
        mounts:
          - source: "{{job_root}}/input"
            target: /input
`, absoluteRoot)
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	commands := cfg.Watches[0].Pipeline
	if want := filepath.Join(absoluteRoot, "work"); commands[0].WorkingDir != want {
		t.Errorf("absolute working directory = %q, want %q", commands[0].WorkingDir, want)
	}
	if want := filepath.Join(absoluteRoot, "input"); commands[0].Mounts[0].Source != want {
		t.Errorf("absolute mount source = %q, want %q", commands[0].Mounts[0].Source, want)
	}
	if got := commands[0].Mounts[0].Target; got != "/container/input" {
		t.Errorf("value-backed mount target = %q, want /container/input", got)
	}
	if want := filepath.Join(configDirectory, "relative-root", "work"); commands[1].WorkingDir != want {
		t.Errorf("relative working directory = %q, want %q", commands[1].WorkingDir, want)
	}
	if want := filepath.Join(configDirectory, "relative-root", "input"); commands[1].Mounts[0].Source != want {
		t.Errorf("relative mount source = %q, want %q", commands[1].Mounts[0].Source, want)
	}
	if want := filepath.Join("{{dir}}", "job-root", "work"); commands[2].WorkingDir != want {
		t.Errorf("per-job working directory = %q, want %q", commands[2].WorkingDir, want)
	}
	if want := filepath.Join("{{dir}}", "job-root", "input"); commands[2].Mounts[0].Source != want {
		t.Errorf("per-job mount source = %q, want %q", commands[2].Mounts[0].Source, want)
	}
}

func TestEffectiveFingerprintUsesExpandedReusableValues(t *testing.T) {
	directory := t.TempDir()
	write := func(name, contents string) string {
		t.Helper()
		filename := filepath.Join(directory, name)
		if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		return filename
	}
	configuration := func(values, argument string) string {
		return fmt.Sprintf(`
%s
watches:
  - name: incoming
    path: .
    pipeline:
      - name: inspect
        program: inspect
        args: [%q]
`, values, argument)
	}

	paths := []string{
		write("ordered.yaml", configuration("values:\n  root: /srv/shared\n  suffix: data", "{{root}}/{{suffix}}")),
		write("reversed.yaml", configuration("values:\n  suffix: data\n  root: /srv/shared", "{{root}}/{{suffix}}")),
		write("literal.yaml", configuration("", "/srv/shared/data")),
		write("changed.yaml", configuration("values:\n  root: /srv/other\n  suffix: data", "{{root}}/{{suffix}}")),
	}
	fingerprints := make([]string, len(paths))
	for index, filename := range paths {
		cfg, err := Load(filename)
		if err != nil {
			t.Fatalf("Load(%s) error = %v", filepath.Base(filename), err)
		}
		fingerprints[index], err = EffectiveFingerprint(cfg)
		if err != nil {
			t.Fatalf("EffectiveFingerprint(%s) error = %v", filepath.Base(filename), err)
		}
	}

	if fingerprints[0] != fingerprints[1] {
		t.Errorf("value map order changed fingerprint: %q != %q", fingerprints[0], fingerprints[1])
	}
	if fingerprints[0] != fingerprints[2] {
		t.Errorf("value alias and equivalent literal fingerprints differ: %q != %q", fingerprints[0], fingerprints[2])
	}
	if fingerprints[0] == fingerprints[3] {
		t.Errorf("material value change retained fingerprint %q", fingerprints[0])
	}
}
