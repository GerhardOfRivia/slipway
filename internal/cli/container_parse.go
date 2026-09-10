package cli

import (
	"encoding/csv"
	"fmt"
	"path"
	"strings"

	"github.com/GerhardOfRivia/onderzeeer/internal/config"
)

type structuredContainerRun struct {
	image         string
	containerArgs []string
	mounts        []generatedMount
	containerEnv  map[string]string
	command       string
	commandArgs   []string
}

type scannedRunOption struct {
	raw      []string
	mount    generatedMount
	hasEnv   bool
	hasMount bool
}

func parseStructuredContainerRun(executor config.ExecutorType, args []string) (*structuredContainerRun, string) {
	if len(args) == 0 {
		return nil, ""
	}
	optionStart := 0
	switch {
	case args[0] == "run":
		optionStart = 1
	case len(args) >= 2 && args[0] == "container" && args[1] == "run":
		optionStart = 2
	default:
		return nil, ""
	}

	environmentSafe := true
	mountsSafe := true
	environment := make(map[string]string)
	seenEnvironment := make(map[string]struct{})
	var options []scannedRunOption

	index := optionStart
	for index < len(args) {
		argument := args[index]
		if argument == "--" {
			index++
			if index >= len(args) {
				return nil, fmt.Sprintf("%s run has no image after --", executor)
			}
			if strings.HasPrefix(args[index], "-") {
				return nil, fmt.Sprintf("%s run needs -- to distinguish image %q", executor, args[index])
			}
			break
		}
		if !strings.HasPrefix(argument, "-") || argument == "-" {
			break
		}

		option, reason := config.ParseContainerRunOption(executor, args[index:])
		if reason != "" {
			return nil, fmt.Sprintf("cannot safely parse %s run option %q: %s", executor, argument, reason)
		}
		raw := append([]string(nil), args[index:index+option.Consumed]...)
		scanned := scannedRunOption{raw: raw}
		if !option.Known {
			// An attached value makes the image boundary knowable, but an
			// unknown option could still interact with extracted fields. Keep
			// every mount and environment option in its original position.
			environmentSafe = false
			mountsSafe = false
		}

		switch option.Name {
		case "env":
			if option.Clustered {
				environmentSafe = false
				break
			}
			key, value, ok := strings.Cut(option.Value, "=")
			_, duplicate := seenEnvironment[key]
			if !ok || key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 || duplicate {
				environmentSafe = false
			} else {
				seenEnvironment[key] = struct{}{}
				environment[key] = value
				scanned.hasEnv = true
			}
		case "mount":
			mount, ok := parseLongBindMount(option.Value)
			if !ok {
				mountsSafe = false
			} else {
				scanned.mount = mount
				scanned.hasMount = true
			}
		case "volume":
			if option.Clustered {
				mountsSafe = false
				break
			}
			mount, ok := parseVolumeBindMount(option.Value)
			if !ok {
				mountsSafe = false
			} else {
				scanned.mount = mount
				scanned.hasMount = true
			}
		}

		switch option.Name {
		case "env-file", "env-host", "env-merge", "http-proxy", "unsetenv", "unsetenv-all":
			environmentSafe = false
		case "tmpfs", "volumes-from":
			mountsSafe = false
		case "secret":
			environmentSafe = false
			mountsSafe = false
		}
		options = append(options, scanned)
		index += option.Consumed
	}

	if index >= len(args) || strings.TrimSpace(args[index]) == "" {
		return nil, fmt.Sprintf("%s run has no non-blank image", executor)
	}

	result := &structuredContainerRun{image: args[index]}
	if environmentSafe && len(environment) > 0 {
		result.containerEnv = environment
	}
	for _, option := range options {
		extractEnvironment := environmentSafe && option.hasEnv
		extractMount := mountsSafe && option.hasMount
		if extractMount {
			result.mounts = append(result.mounts, option.mount)
		}
		if extractEnvironment || extractMount {
			continue
		}
		for _, token := range option.raw {
			if token == "--" {
				return nil, fmt.Sprintf("%s run option %q has a value that cannot be represented in container_args", executor, option.raw[0])
			}
		}
		result.containerArgs = append(result.containerArgs, option.raw...)
	}

	if index+1 < len(args) {
		if strings.TrimSpace(args[index+1]) == "" || strings.HasPrefix(args[index+1], "-") {
			result.commandArgs = append([]string(nil), args[index+1:]...)
			return result, ""
		}
		result.command = args[index+1]
		result.commandArgs = append([]string(nil), args[index+2:]...)
	}
	return result, ""
}

func parseLongBindMount(specification string) (generatedMount, bool) {
	reader := csv.NewReader(strings.NewReader(specification))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil || len(records) != 1 {
		return generatedMount{}, false
	}

	var mount generatedMount
	var hasType, hasSource, hasTarget bool
	for _, rawField := range records[0] {
		key, value, hasValue := strings.Cut(rawField, "=")
		switch key {
		case "type":
			if hasType || !hasValue || value != "bind" {
				return generatedMount{}, false
			}
			hasType = true
		case "source", "src":
			if hasSource || !hasValue || strings.TrimSpace(value) == "" {
				return generatedMount{}, false
			}
			mount.Source = value
			hasSource = true
		case "target", "destination", "dest", "dst":
			if hasTarget || !hasValue || !containerMountTargetCanBeAbsolute(value) {
				return generatedMount{}, false
			}
			mount.Target = value
			hasTarget = true
		default:
			if strings.TrimSpace(rawField) == "" || strings.TrimSpace(key) == "" || strings.IndexByte(rawField, 0) >= 0 {
				return generatedMount{}, false
			}
			mount.Options = append(mount.Options, rawField)
		}
	}
	if !hasType || !hasSource || !hasTarget {
		return generatedMount{}, false
	}
	return mount, true
}

func parseVolumeBindMount(specification string) (generatedMount, bool) {
	parts := strings.Split(specification, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return generatedMount{}, false
	}
	source, target := parts[0], parts[1]
	if !hostVolumeSourceLooksLikePath(source) || !containerMountTargetCanBeAbsolute(target) {
		return generatedMount{}, false
	}

	mount := generatedMount{Source: source, Target: target}
	if len(parts) == 2 {
		return mount, true
	}
	if parts[2] == "ro" {
		mount.Options = []string{"ro"}
		return mount, true
	}
	if parts[2] == "rw" {
		return mount, true
	}
	return generatedMount{}, false
}

func hostVolumeSourceLooksLikePath(source string) bool {
	return path.IsAbs(source) || strings.HasPrefix(source, ".") ||
		strings.HasPrefix(source, "{{file}}") || strings.HasPrefix(source, "{{dir}}")
}

func containerMountTargetCanBeAbsolute(target string) bool {
	return path.IsAbs(target) || strings.HasPrefix(target, "{{file}}") || strings.HasPrefix(target, "{{dir}}")
}
