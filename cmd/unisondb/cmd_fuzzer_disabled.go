//go:build !fuzz

package main

import (
	"errors"

	"github.com/urfave/cli/v2"
)

var fuzzCommand = &cli.Command{
	Name:  "fuzz",
	Usage: "Run in fuzz testing mode (disabled in production builds)",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return errors.New("fuzz mode is only available in builds with `-tags fuzz`")
	},
	CustomHelpTemplate: cli.CommandHelpTemplate + "\n\nNOTE: This feature is disabled in regular builds.\n\nTo enable:\n  go build -tags fuzz\n\n",
}
