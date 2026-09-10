package cli

import (
	"bytes"
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"
)

func TestParseFlagsWithPositionalArguments(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		args   []string
		want   []string
		socket string
		remove bool
	}{
		{name: "positional only", args: []string{"pipeline.yaml", "worker"}, want: []string{"pipeline.yaml", "worker"}},
		{name: "options first", args: []string{"--rm", "--socket", "control.sock", "pipeline.yaml", "worker"}, want: []string{"pipeline.yaml", "worker"}, socket: "control.sock", remove: true},
		{name: "options last", args: []string{"pipeline.yaml", "worker", "--socket", "control.sock", "--rm"}, want: []string{"pipeline.yaml", "worker"}, socket: "control.sock", remove: true},
		{name: "options between", args: []string{"pipeline.yaml", "--socket=control.sock", "worker", "--rm=false"}, want: []string{"pipeline.yaml", "worker"}, socket: "control.sock"},
		{name: "preserve spaces", args: []string{"pipeline with spaces.yaml", "--socket", "control socket", "worker"}, want: []string{"pipeline with spaces.yaml", "worker"}, socket: "control socket"},
		{name: "option terminator", args: []string{"--socket", "control.sock", "--", "-pipeline.yaml", "--rm"}, want: []string{"-pipeline.yaml", "--rm"}, socket: "control.sock"},
		{name: "value begins with dash", args: []string{"pipeline.yaml", "--socket", "-control.sock", "worker"}, want: []string{"pipeline.yaml", "worker"}, socket: "-control.sock"},
		{name: "repeated option", args: []string{"--socket", "first.sock", "pipeline.yaml", "worker", "--socket=last.sock"}, want: []string{"pipeline.yaml", "worker"}, socket: "last.sock"},
		{name: "empty daemon", args: []string{"--socket", "control.sock"}, socket: "control.sock"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			flags := newFlagSet("test", &output, "test [options] <config> <name>")
			socket := flags.String("socket", "", "control socket")
			remove := flags.Bool("rm", false, "remove instance")
			if err := parseFlags(flags, test.args); err != nil {
				t.Fatal(err)
			}
			if got := flags.Args(); len(got) != len(test.want) || len(got) > 0 && !reflect.DeepEqual(got, test.want) {
				t.Errorf("positional arguments = %q, want %q", got, test.want)
			}
			if *socket != test.socket || *remove != test.remove {
				t.Errorf("socket/remove = %q/%v, want %q/%v", *socket, *remove, test.socket, test.remove)
			}
		})
	}
}

func TestParseFlagsReportsErrorsAndHelpAfterPositionalArguments(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		args []string
		want string
		help bool
	}{
		{args: []string{"pipeline.yaml", "worker", "--socket"}, want: "flag needs an argument"},
		{args: []string{"pipeline.yaml", "worker", "--unknown"}, want: "flag provided but not defined"},
		{args: []string{"pipeline.yaml", "worker", "--rm=invalid"}, want: "invalid boolean value"},
		{args: []string{"pipeline.yaml", "worker", "--help"}, want: "Usage: test", help: true},
	} {
		var output bytes.Buffer
		flags := newFlagSet("test", &output, "test [options] <config> <name>")
		flags.String("socket", "", "control socket")
		flags.Bool("rm", false, "remove instance")
		err := parseFlags(flags, test.args)
		if err == nil || errors.Is(err, flag.ErrHelp) != test.help || !strings.Contains(output.String(), test.want) {
			t.Errorf("parseFlags(%v) error/output = %v/%q, want %q", test.args, err, output.String(), test.want)
		}
	}
}

func TestCommandsRejectExtraPositionalArguments(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"check", "test", "start", "status", "queue", "jobs", "job", "logs", "onderzeeerd"} {
		args := []string{"pipeline.yaml"}
		if command == "start" || command == "onderzeeerd" {
			args = append(args, "worker")
		}
		if command == "job" || command == "logs" {
			args = append(args, "1")
		}
		args = append(args, "unexpected")
		var stdout, stderr bytes.Buffer
		var code int
		if command == "onderzeeerd" {
			code = RunDaemon(args, &stdout, &stderr)
		} else {
			code = Run(append([]string{command}, args...), &stdout, &stderr)
		}
		if code != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "positional argument") {
			t.Errorf("%s %v = %d, stdout %q, stderr %q; want extra argument usage error", command, args, code, stdout.String(), stderr.String())
		}
	}
}
