package config

import (
	"fmt"
	"sort"
	"strings"
)

var reservedTemplateValueNames = map[string]struct{}{
	"file":     {},
	"dir":      {},
	"basename": {},
	"stem":     {},
	"ext":      {},
	"job_id":   {},
}

// expandValues resolves config-local values and applies them to the pipeline
// fields that also support per-job templates. Per-job placeholders remain for
// the worker's later expansion phase.
func (c *Config) expandValues() error {
	resolved, err := resolveTemplateValues(c.Values)
	if err != nil {
		return err
	}
	c.Values = resolved
	if len(resolved) == 0 {
		return nil
	}

	replacer := newTemplateValueReplacer(resolved)
	for watchIndex := range c.Watches {
		for commandIndex := range c.Watches[watchIndex].Pipeline {
			command := &c.Watches[watchIndex].Pipeline[commandIndex]
			command.Args = replaceTemplateValueStrings(command.Args, replacer)
			command.Image = replacer.Replace(command.Image)
			command.ContainerArgs = replaceTemplateValueStrings(command.ContainerArgs, replacer)
			command.Command = replacer.Replace(command.Command)
			command.CommandArgs = replaceTemplateValueStrings(command.CommandArgs, replacer)
			command.WorkingDir = replacer.Replace(command.WorkingDir)
			command.Output = replacer.Replace(command.Output)
			for mountIndex := range command.Mounts {
				mount := &command.Mounts[mountIndex]
				mount.Source = replacer.Replace(mount.Source)
				mount.Target = replacer.Replace(mount.Target)
				mount.Options = replaceTemplateValueStrings(mount.Options, replacer)
			}
			command.ContainerEnv = replaceTemplateValueMap(command.ContainerEnv, replacer)
			command.Env = replaceTemplateValueMap(command.Env, replacer)
		}
	}
	return nil
}

func resolveTemplateValues(values map[string]string) (map[string]string, error) {
	if values == nil {
		return nil, nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		if !validTemplateValueName(key) {
			return nil, fmt.Errorf("values contains invalid key %q; keys must start with a letter or underscore and contain only letters, digits, and underscores", key)
		}
		if _, reserved := reservedTemplateValueNames[key]; reserved {
			return nil, fmt.Errorf("values key %q is reserved for the built-in template {{%s}}", key, key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	resolved := make(map[string]string, len(values))
	state := make(map[string]uint8, len(values))
	stack := make([]string, 0, len(values))
	var resolve func(string) (string, error)
	resolve = func(key string) (string, error) {
		switch state[key] {
		case 2:
			return resolved[key], nil
		case 1:
			cycleStart := 0
			for cycleStart < len(stack) && stack[cycleStart] != key {
				cycleStart++
			}
			cycle := append(append([]string(nil), stack[cycleStart:]...), key)
			return "", fmt.Errorf("values contain a reference cycle: %s", strings.Join(cycle, " -> "))
		}

		state[key] = 1
		stack = append(stack, key)
		value := values[key]
		var replacements []string
		for _, candidate := range keys {
			token := "{{" + candidate + "}}"
			if !strings.Contains(value, token) {
				continue
			}
			replacement, err := resolve(candidate)
			if err != nil {
				return "", err
			}
			replacements = append(replacements, token, replacement)
		}
		if len(replacements) > 0 {
			value = strings.NewReplacer(replacements...).Replace(value)
		}
		stack = stack[:len(stack)-1]
		state[key] = 2
		resolved[key] = value
		return value, nil
	}

	for _, key := range keys {
		if _, err := resolve(key); err != nil {
			return nil, err
		}
	}
	return resolved, nil
}

func validTemplateValueName(name string) bool {
	if name == "" {
		return false
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		letter := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
		if index == 0 {
			if !letter && character != '_' {
				return false
			}
			continue
		}
		if !letter && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func newTemplateValueReplacer(values map[string]string) *strings.Replacer {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	replacements := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		replacements = append(replacements, "{{"+key+"}}", values[key])
	}
	return strings.NewReplacer(replacements...)
}

func replaceTemplateValueStrings(values []string, replacer *strings.Replacer) []string {
	for index := range values {
		values[index] = replacer.Replace(values[index])
	}
	return values
}

func replaceTemplateValueMap(values map[string]string, replacer *strings.Replacer) map[string]string {
	for key, value := range values {
		values[key] = replacer.Replace(value)
	}
	return values
}
