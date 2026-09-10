package cli

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"

	"github.com/GerhardOfRivia/slipway/internal/config"
	"github.com/GerhardOfRivia/slipway/internal/control"
)

type inspectionOptions struct {
	local  *bool
	socket *string
}

func inspectionFlags(flags *flag.FlagSet) inspectionOptions {
	return inspectionOptions{
		local:  flags.Bool("local", false, "read the config's standalone queue database directly"),
		socket: flags.String("socket", "", "daemon control socket (defaults to SLIPWAY_SOCKET or a per-user path)"),
	}
}

func (options inspectionOptions) open(selection string) ([]configuredStore, error) {
	if *options.local {
		if *options.socket != "" {
			return nil, usageError{message: "--local and --socket cannot be combined"}
		}
		return openStores(selection)
	}
	// Paths are relative to the client, even when the daemon runs elsewhere.
	// Names/IDs are sent unchanged and do not require any client-side files.
	info, statErr := os.Stat(selection)
	if statErr == nil && (info.IsDir() || info.Mode().IsRegular()) || strings.ContainsAny(selection, `/\`) || strings.HasSuffix(selection, ".yaml") || strings.HasSuffix(selection, ".yml") || selection == "." || selection == ".." {
		absolute, err := filepath.Abs(selection)
		if err != nil {
			return nil, err
		}
		selection = absolute
	}
	client := control.NewClient(*options.socket)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()
	instances, err := client.SelectQueues(ctx, selection)
	if err != nil {
		return nil, err
	}
	stores := make([]configuredStore, 0, len(instances))
	for _, instance := range instances {
		stores = append(stores, configuredStore{
			path:   instance.ConfigPath,
			config: &config.Config{Database: config.DatabaseConfig{Path: instance.DatabasePath}},
			store:  control.NewQueueReader(*options.socket, instance.ID),
		})
	}
	return stores, nil
}
