//go:build !fuzz

package main

import "github.com/urfave/cli/v2"

var replicaCommand = &cli.Command{
	Name:  "replica",
	Usage: "Run as read-only replica (syncs from upstream, no downstream serving)",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return Run(c.Context, c.String("config"), c.String("env"),
			"replica", c.String("ports-file"))
	},
}
