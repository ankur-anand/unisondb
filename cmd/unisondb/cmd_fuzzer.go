//go:build fuzz

package main

import (
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
)

var fuzzCommand = &cli.Command{
	Name:  "fuzz",
	Usage: "Run in fuzz testing mode (should only be used for testing)",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return Run(c.Context, c.String("config"), c.String("env"),
			"fuzz", c.String("ports-file"))
	},
}

var serverCommand = &cli.Command{
	Name:  "server",
	Usage: "Server mode is disabled in fuzz builds",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return errors.New("server mode is disabled in this build (built with -tags fuzz)")
	},
	CustomHelpTemplate: fmt.Sprintf(`%s

NOTE: This binary was built with -tags fuzz. Server mode is disabled for safety.
`, cli.CommandHelpTemplate),
}

var replicaCommand = &cli.Command{
	Name:  "replica",
	Usage: "Replica mode is disabled in fuzz builds",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return errors.New("replica mode is disabled in this build (built with -tags fuzz)")
	},
	CustomHelpTemplate: fmt.Sprintf(`%s

NOTE: This binary was built with -tags fuzz. Replica mode is disabled for safety.
`, cli.CommandHelpTemplate),
}

var relayCommand = &cli.Command{
	Name:  "relay",
	Usage: "Relay mode is disabled in fuzz builds",
	Flags: commonFlags,
	Action: func(c *cli.Context) error {
		return errors.New("relay mode is disabled in this build (built with -tags fuzz)")
	},
	CustomHelpTemplate: fmt.Sprintf(`%s

NOTE: This binary was built with -tags fuzz. Relay mode is disabled for safety.
`, cli.CommandHelpTemplate),
}
