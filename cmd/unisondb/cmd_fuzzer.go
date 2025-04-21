//go:build fuzz

package main

import (
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
)

var fuzzerCommand = &cli.Command{
	Name:  "fuzzer",
	Usage: "Run in fuzzer mode, (should only be used for testing)",
	Action: func(c *cli.Context) error {
		return Run(c.Context, c.String("config"), c.String("env"),
			"fuzzer", c.Bool("grpc"))
	},
}

var replicatorCommand = &cli.Command{
	Name:  "replicator",
	Usage: "Replicator mode is disabled in fuzz builds",
	Action: func(c *cli.Context) error {
		return errors.New("replicator mode is disabled in this build (built with -tags fuzz)")
	},
	CustomHelpTemplate: fmt.Sprintf(`%s

NOTE: This binary was built with -tags fuzz. Replicator mode is disabled for safety.
`, cli.CommandHelpTemplate),
}
