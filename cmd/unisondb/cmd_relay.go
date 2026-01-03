//go:build !fuzz

package main

import "github.com/urfave/cli/v2"

var relayCommand = &cli.Command{
	Name:  "relay",
	Usage: "Run as relay node (syncs from upstream, serves downstream via gRPC)",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return Run(c.Context, c.String("config"), c.String("env"),
			"relay", c.String("ports-file"))
	},
}
