//go:build !fuzz

package main

import "github.com/urfave/cli/v2"

var serverCommand = &cli.Command{
	Name:  "server",
	Usage: "Run as primary server (accepts writes)",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return Run(c.Context, c.String("config"), c.String("env"),
			"server", c.String("ports-file"))
	},
}
