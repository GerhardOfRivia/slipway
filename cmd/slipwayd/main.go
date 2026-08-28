package main

import (
	"os"

	"github.com/GerhardOfRivia/slipway/internal/cli"
)

// Version is replaced at build time with -ldflags "-X main.Version=...".
var Version = "dev"

func main() {
	os.Exit(cli.RunDaemonVersion(os.Args[1:], os.Stdout, os.Stderr, Version))
}
