package cli

import (
	"fmt"
	"io"
	"strings"
)

func checkCommand(args []string, stdout, stderr io.Writer) error {
	flags := newFlagSet("check", stderr, "slipway check [--raw] [--config path]")
	configPath := flags.String("config", configPathDefault(), "YAML configuration file or directory")
	raw := flags.Bool("raw", false, "display the exact program and JSON argument array")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError{message: "check does not accept positional arguments"}
	}

	configs, err := loadConfigs(*configPath)
	if err != nil {
		return err
	}
	return printPipelines(stdout, configs, *raw)
}

func printPipelines(output io.Writer, configs []loadedConfig, raw bool) error {
	formatCommand := formatShellInvocation
	if raw {
		formatCommand = formatInvocation
	}
	for configIndex, item := range configs {
		if configIndex > 0 {
			if _, err := fmt.Fprintln(output); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(output, "Config: %s\n", item.path); err != nil {
			return err
		}
		for _, watch := range item.config.Watches {
			if _, err := fmt.Fprintf(output, "Watch: %s\n", watch.Name); err != nil {
				return err
			}
			for index, command := range watch.Pipeline {
				if _, err := fmt.Fprintf(output, "  %d. %s [%s]: %s\n",
					index+1, command.Name, command.Executor, formatCommand(command.Program, command.ExecutionArgs())); err != nil {
					return err
				}
				if command.Output != "" {
					if _, err := fmt.Fprintf(output, "     output: %q\n", command.Output); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func formatShellInvocation(program string, args []string) string {
	values := make([]string, 0, len(args)+1)
	values = append(values, quoteShellCommand(program))
	for _, argument := range args {
		values = append(values, quoteShellWord(argument))
	}
	return strings.Join(values, " ")
}

func quoteShellCommand(value string) string {
	if strings.ContainsRune(value, '=') || isShellKeyword(value) {
		return quoteShellWordAlways(value)
	}
	return quoteShellWord(value)
}

func quoteShellWord(value string) string {
	if value == "" {
		return "''"
	}
	if isShellSafeWord(value) {
		return value
	}
	return quoteShellWordAlways(value)
}

func quoteShellWordAlways(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func isShellKeyword(value string) bool {
	switch value {
	case "case", "coproc", "do", "done", "elif", "else", "esac", "fi", "for", "function", "if", "in", "select", "then", "time", "until", "while":
		return true
	default:
		return false
	}
}

func isShellSafeWord(value string) bool {
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case strings.ContainsRune("_@%+=:,./-", character):
		default:
			return false
		}
	}
	return true
}
