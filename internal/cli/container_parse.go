package cli

import (
	"encoding/csv"
	"fmt"
	"path"
	"strings"

	"github.com/GerhardOfRivia/slipway/internal/config"
)

type structuredContainerRun struct {
	image         string
	containerArgs []string
	mounts        []generatedMount
	containerEnv  map[string]string
	command       string
	commandArgs   []string
}

type runOptionArity uint8

const (
	runOptionBoolean runOptionArity = iota
	runOptionValue
)

type runOptionGrammar struct {
	long  map[string]runOptionArity
	short map[byte]runOptionArity
}

type parsedRunOption struct {
	name      string
	value     string
	consumed  int
	known     bool
	clustered bool
}

type scannedRunOption struct {
	raw      []string
	mount    generatedMount
	hasEnv   bool
	hasMount bool
}

var dockerRunGrammar = newRunOptionGrammar(
	// Value-taking options from Docker's run reference. Deprecated aliases are
	// included because the CLI still accepts them even when help omits them.
	`add-host annotation attach blkio-weight blkio-weight-device cap-add cap-drop
	 cgroup-parent cgroupns cidfile cpu-count cpu-percent cpu-period cpu-quota
	 cpu-rt-period cpu-rt-runtime cpu-shares cpus cpuset-cpus cpuset-mems
	 detach-keys device device-cgroup-rule device-read-bps device-read-iops
	 device-write-bps device-write-iops dns dns-opt dns-option dns-search
	 domainname entrypoint env env-file expose gpus group-add health-cmd
	 health-interval health-retries health-start-interval health-start-period
	 health-timeout hostname io-maxbandwidth io-maxiops ip ip6 ipc isolation
	 kernel-memory label label-file link link-local-ip log-driver log-opt
	 mac-address memory memory-reservation memory-swap memory-swappiness mount
	 name net net-alias network network-alias oom-score-adj pid pids-limit
	 platform publish pull restart runtime security-opt shm-size stop-signal
	 stop-timeout storage-opt sysctl tmpfs ulimit user userns uts volume
	 volume-driver volumes-from workdir`,
	`detach disable-content-trust help init interactive no-healthcheck
	 oom-kill-disable privileged publish-all quiet read-only rm sig-proxy tty
	 use-api-socket`,
	"acehlmpuvw",
	"diPqt",
)

var podmanRunGrammar = extendRunOptionGrammar(
	dockerRunGrammar,
	`arch authfile cert-dir cgroup-conf cgroups chrootdirs conmon-pidfile creds
	 decryption-key env-merge gidmap group-entry health-log-destination
	 health-max-log-count health-max-log-size health-on-failure health-startup-cmd
	 health-startup-interval health-startup-retries health-startup-success
	 health-startup-timeout hosts-file hostuser image-volume init-path os
	 passwd-entry personality pidfile pod pod-id-file preserve-fd
	 preserve-fds rdt-class requires retry retry-delay sdnotify seccomp-policy
	 secret shm-size-systemd signature-policy subgidname subuidname systemd
	 timeout tz uidmap umask unsetenv variant`,
	`env-host http-proxy no-hostname no-hosts read-only-tmpfs replace rmi rootfs
	 passwd tls-verify unsetenv-all`,
)

func newRunOptionGrammar(valueLong, booleanLong, valueShort, booleanShort string) runOptionGrammar {
	grammar := runOptionGrammar{
		long:  make(map[string]runOptionArity),
		short: make(map[byte]runOptionArity),
	}
	addLongRunOptions(grammar.long, strings.Fields(valueLong), runOptionValue)
	addLongRunOptions(grammar.long, strings.Fields(booleanLong), runOptionBoolean)
	for index := range valueShort {
		grammar.short[valueShort[index]] = runOptionValue
	}
	for index := range booleanShort {
		grammar.short[booleanShort[index]] = runOptionBoolean
	}
	return grammar
}

func extendRunOptionGrammar(base runOptionGrammar, valueLong, booleanLong string) runOptionGrammar {
	grammar := runOptionGrammar{
		long:  make(map[string]runOptionArity, len(base.long)),
		short: make(map[byte]runOptionArity, len(base.short)),
	}
	for name, arity := range base.long {
		grammar.long[name] = arity
	}
	for name, arity := range base.short {
		grammar.short[name] = arity
	}
	addLongRunOptions(grammar.long, strings.Fields(valueLong), runOptionValue)
	addLongRunOptions(grammar.long, strings.Fields(booleanLong), runOptionBoolean)
	return grammar
}

func addLongRunOptions(options map[string]runOptionArity, names []string, arity runOptionArity) {
	for _, name := range names {
		options[name] = arity
	}
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

	grammar := dockerRunGrammar
	if executor == config.ExecutorPodman {
		grammar = podmanRunGrammar
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

		option, reason := parseContainerRunOption(grammar, args[index:])
		if reason != "" {
			return nil, fmt.Sprintf("cannot safely parse %s run option %q: %s", executor, argument, reason)
		}
		raw := append([]string(nil), args[index:index+option.consumed]...)
		scanned := scannedRunOption{raw: raw}
		if !option.known {
			// An attached value makes the image boundary knowable, but an
			// unknown option could still interact with extracted fields. Keep
			// every mount and environment option in its original position.
			environmentSafe = false
			mountsSafe = false
		}

		switch option.name {
		case "env":
			if option.clustered {
				environmentSafe = false
				break
			}
			key, value, ok := strings.Cut(option.value, "=")
			_, duplicate := seenEnvironment[key]
			if !ok || key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 || duplicate {
				environmentSafe = false
			} else {
				seenEnvironment[key] = struct{}{}
				environment[key] = value
				scanned.hasEnv = true
			}
		case "mount":
			mount, ok := parseLongBindMount(option.value)
			if !ok {
				mountsSafe = false
			} else {
				scanned.mount = mount
				scanned.hasMount = true
			}
		case "volume":
			if option.clustered {
				mountsSafe = false
				break
			}
			mount, ok := parseVolumeBindMount(option.value)
			if !ok {
				mountsSafe = false
			} else {
				scanned.mount = mount
				scanned.hasMount = true
			}
		}

		switch option.name {
		case "env-file", "env-host", "env-merge", "http-proxy", "unsetenv", "unsetenv-all":
			environmentSafe = false
		case "tmpfs", "volumes-from":
			mountsSafe = false
		case "secret":
			environmentSafe = false
			mountsSafe = false
		}
		options = append(options, scanned)
		index += option.consumed
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

func parseContainerRunOption(grammar runOptionGrammar, args []string) (parsedRunOption, string) {
	argument := args[0]
	if strings.HasPrefix(argument, "--") {
		nameValue := strings.TrimPrefix(argument, "--")
		if nameValue == "" {
			return parsedRunOption{}, "option name is blank"
		}
		if name, value, hasValue := strings.Cut(nameValue, "="); hasValue {
			if name == "" {
				return parsedRunOption{}, "option name is blank"
			}
			_, known := grammar.long[name]
			return parsedRunOption{name: name, value: value, consumed: 1, known: known}, ""
		}

		arity, known := grammar.long[nameValue]
		if !known {
			return parsedRunOption{}, "unknown option arity"
		}
		if arity == runOptionBoolean {
			return parsedRunOption{name: nameValue, consumed: 1, known: true}, ""
		}
		if len(args) < 2 {
			return parsedRunOption{}, "value is missing"
		}
		return parsedRunOption{name: nameValue, value: args[1], consumed: 2, known: true}, ""
	}

	shorthands := strings.TrimPrefix(argument, "-")
	if shorthands == "" {
		return parsedRunOption{}, "option name is blank"
	}
	for index := 0; index < len(shorthands); index++ {
		shortName := shorthands[index]
		arity, known := grammar.short[shortName]
		if !known {
			return parsedRunOption{}, fmt.Sprintf("unknown shorthand -%c", shortName)
		}
		if arity == runOptionBoolean {
			if index+1 < len(shorthands) && shorthands[index+1] == '=' {
				return parsedRunOption{consumed: 1, known: true}, ""
			}
			continue
		}

		value := shorthands[index+1:]
		hasAttachedValue := value != ""
		consumed := 1
		if strings.HasPrefix(value, "=") {
			value = strings.TrimPrefix(value, "=")
		}
		if !hasAttachedValue {
			if len(args) < 2 {
				return parsedRunOption{}, fmt.Sprintf("value for -%c is missing", shortName)
			}
			value = args[1]
			consumed = 2
		}
		name := ""
		switch shortName {
		case 'e':
			name = "env"
		case 'v':
			name = "volume"
		}
		return parsedRunOption{name: name, value: value, consumed: consumed, known: true, clustered: name != "" && index != 0}, ""
	}
	return parsedRunOption{consumed: 1, known: true}, ""
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
