//go:build !fuzz

package main

import (
	"errors"

	"github.com/urfave/cli/v2"
)

var fuzzerCommand = &cli.Command{
	Name:  "fuzzer",
	Usage: "This is a testing-only feature (disabled in production builds)",
	Action: func(c *cli.Context) error {
		return errors.New("fuzzer mode is only available in builds with `-tags fuzz`")
	},
	CustomHelpTemplate: cli.CommandHelpTemplate + "\n\nNOTE: This feature is disabled in regular builds.\n\nTo enable:\n  go build -tags fuzz\n\n",
}

var replicatorCommand = &cli.Command{
	Name:  "replicator",
	Usage: "Run in replicator mode",
	Action: func(c *cli.Context) error {
		return Run(c.Context, c.String("config"), c.String("env"),
			"replicator", c.Bool("grpc"))
	},
}
