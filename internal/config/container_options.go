package config

import (
	"fmt"
	"strconv"
	"strings"
)

type runOptionArity uint8

const (
	runOptionBoolean runOptionArity = iota
	runOptionValue
)

type runOptionGrammar struct {
	long  map[string]runOptionArity
	short map[byte]runOptionArity
}

// ContainerRunOption describes one runtime option and how many argv entries it consumes.
type ContainerRunOption struct {
	Name      string
	Value     string
	Consumed  int
	Known     bool
	Clustered bool

	interactive *bool
	tty         *bool
	detach      *bool
}

func (option *ContainerRunOption) recordTerminalBoolean(name, value string) {
	var target **bool
	switch name {
	case "interactive", "i":
		target = &option.interactive
	case "tty", "t":
		target = &option.tty
	case "detach", "d":
		target = &option.detach
	default:
		return
	}
	if enabled, err := strconv.ParseBool(value); err == nil {
		*target = &enabled
	}
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

// ParseContainerRunOption scans one Docker or Podman option without consuming
// the image or application arguments. The reason is nonempty if arity is unknown.
func ParseContainerRunOption(executor ExecutorType, args []string) (ContainerRunOption, string) {
	grammar := dockerRunGrammar
	if executor == ExecutorPodman {
		grammar = podmanRunGrammar
	}
	if len(args) == 0 {
		return ContainerRunOption{}, "option is missing"
	}
	argument := args[0]
	if strings.HasPrefix(argument, "--") {
		nameValue := strings.TrimPrefix(argument, "--")
		if nameValue == "" {
			return ContainerRunOption{}, "option name is blank"
		}
		if name, value, hasValue := strings.Cut(nameValue, "="); hasValue {
			if name == "" {
				return ContainerRunOption{}, "option name is blank"
			}
			_, known := grammar.long[name]
			option := ContainerRunOption{Name: name, Value: value, Consumed: 1, Known: known}
			option.recordTerminalBoolean(name, value)
			return option, ""
		}

		arity, known := grammar.long[nameValue]
		if !known {
			return ContainerRunOption{}, "unknown option arity"
		}
		if arity == runOptionBoolean {
			option := ContainerRunOption{Name: nameValue, Consumed: 1, Known: true}
			option.recordTerminalBoolean(nameValue, "true")
			return option, ""
		}
		if len(args) < 2 {
			return ContainerRunOption{}, "value is missing"
		}
		return ContainerRunOption{Name: nameValue, Value: args[1], Consumed: 2, Known: true}, ""
	}

	shorthands := strings.TrimPrefix(argument, "-")
	if shorthands == "" {
		return ContainerRunOption{}, "option name is blank"
	}
	option := ContainerRunOption{Consumed: 1, Known: true}
	for index := 0; index < len(shorthands); index++ {
		shortName := shorthands[index]
		arity, known := grammar.short[shortName]
		if !known {
			return ContainerRunOption{}, fmt.Sprintf("unknown shorthand -%c", shortName)
		}
		if arity == runOptionBoolean {
			if index+1 < len(shorthands) && shorthands[index+1] == '=' {
				option.recordTerminalBoolean(string(shortName), shorthands[index+2:])
				return option, ""
			}
			option.recordTerminalBoolean(string(shortName), "true")
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
				return ContainerRunOption{}, fmt.Sprintf("value for -%c is missing", shortName)
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
		option.Name, option.Value, option.Consumed = name, value, consumed
		option.Clustered = name != "" && index != 0
		return option, ""
	}
	return option, ""
}
