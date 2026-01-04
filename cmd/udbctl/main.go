package main

import (
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/ankur-anand/unisondb/internal/udbctl/output"
	"github.com/ankur-anand/unisondb/internal/udbctl/restore"
	"github.com/ankur-anand/unisondb/internal/udbctl/wal"
	"github.com/urfave/cli/v2"
)

var formatFlag = &cli.StringFlag{
	Name:    "format",
	Aliases: []string{"f"},
	Value:   "table",
	Usage:   "Output format: table, json",
}

func getFormatter(c *cli.Context) (output.Formatter, error) {
	format := c.String("format")
	if format != "table" && format != "json" {
		return nil, fmt.Errorf("invalid format %q: must be 'table' or 'json'", format)
	}
	return output.NewFormatter(output.Format(format)), nil
}

func main() {
	app := &cli.App{
		Name:    "udbctl",
		Usage:   "UnisonDB control CLI",
		Version: "0.1.0",
		Commands: []*cli.Command{
			walCommand(),
			restoreCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func walCommand() *cli.Command {
	return &cli.Command{
		Name:  "wal",
		Usage: "WAL inspection commands",
		Subcommands: []*cli.Command{
			{
				Name:  "list",
				Usage: "List all WAL segments",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "wal-dir",
						Aliases:  []string{"w"},
						Required: true,
						Usage:    "Path to WAL directory",
					},
					formatFlag,
				},
				Action: walListAction,
			},
			{
				Name:  "inspect",
				Usage: "Inspect a specific WAL segment",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "wal-dir",
						Aliases:  []string{"w"},
						Required: true,
						Usage:    "Path to WAL directory",
					},
					&cli.UintFlag{
						Name:        "segment-id",
						Aliases:     []string{"s"},
						Required:    true,
						Usage:       "Segment ID to inspect",
						DefaultText: "required",
					},
					&cli.BoolFlag{
						Name:  "show-index",
						Usage: "Show index entries",
					},
					formatFlag,
				},
				Action: walInspectAction,
			},
			{
				Name:  "stats",
				Usage: "Show aggregate WAL statistics",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "wal-dir",
						Aliases:  []string{"w"},
						Required: true,
						Usage:    "Path to WAL directory",
					},
					formatFlag,
				},
				Action: walStatsAction,
			},
		},
	}
}

func restoreCommand() *cli.Command {
	return &cli.Command{
		Name:  "restore",
		Usage: "Restore B-Tree and/or WAL from backup (requires server stopped)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "data-dir",
				Aliases:  []string{"d"},
				Required: true,
				Usage:    "Target data directory",
			},
			&cli.StringFlag{
				Name:     "namespace",
				Aliases:  []string{"n"},
				Required: true,
				Usage:    "Namespace to restore",
			},
			&cli.StringFlag{
				Name:  "btree",
				Usage: "Path to B-Tree backup file",
			},
			&cli.StringFlag{
				Name:  "wal",
				Usage: "Path to WAL backup directory",
			},
			&cli.BoolFlag{
				Name:  "force",
				Usage: "Confirm server is stopped (required for actual restore)",
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Validate backup files without restoring",
			},
			formatFlag,
		},
		Action: restoreAction,
	}
}

func walListAction(c *cli.Context) error {
	formatter, err := getFormatter(c)
	if err != nil {
		return err
	}

	segments, err := wal.ListSegments(c.String("wal-dir"))
	if err != nil {
		return err
	}

	return formatter.WriteSegmentList(os.Stdout, segments)
}

func walInspectAction(c *cli.Context) error {
	formatter, err := getFormatter(c)
	if err != nil {
		return err
	}

	segmentIDVal := c.Uint("segment-id")
	if segmentIDVal > math.MaxUint32 {
		return fmt.Errorf("segment-id %d exceeds maximum value %d", segmentIDVal, math.MaxUint32)
	}

	detail, err := wal.InspectSegment(c.String("wal-dir"), uint32(segmentIDVal), c.Bool("show-index"))
	if err != nil {
		return err
	}

	return formatter.WriteSegmentDetail(os.Stdout, *detail)
}

func walStatsAction(c *cli.Context) error {
	formatter, err := getFormatter(c)
	if err != nil {
		return err
	}

	stats, err := wal.GetStats(c.String("wal-dir"))
	if err != nil {
		return err
	}

	return formatter.WriteWalStats(os.Stdout, *stats)
}

func restoreAction(c *cli.Context) error {
	formatter, err := getFormatter(c)
	if err != nil {
		return err
	}

	dryRun := c.Bool("dry-run")

	if !dryRun && !c.Bool("force") {
		return errors.New("--force flag required to confirm server is stopped (use --dry-run to preview)")
	}

	btreePath := c.String("btree")
	walPath := c.String("wal")
	if btreePath == "" && walPath == "" {
		return errors.New("must specify --btree and/or --wal")
	}

	opts := restore.Options{
		DataDir:   c.String("data-dir"),
		Namespace: c.String("namespace"),
		BTreePath: btreePath,
		WALPath:   walPath,
		DryRun:    dryRun,
	}

	if !dryRun {
		fmt.Println("WARNING: This will overwrite existing data!")
		fmt.Printf("  Data Dir:  %s\n", opts.DataDir)
		fmt.Printf("  Namespace: %s\n", opts.Namespace)
		fmt.Println()
	}

	result, err := restore.Restore(opts)
	if err != nil {
		return err
	}

	return formatter.WriteRestoreResult(os.Stdout, *result)
}
