package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/urfave/cli/v2"
	"github.com/vylet-app/go/api/server"
)

func main() {
	app := cli.App{
		Name: "api",
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			&cli.StringFlag{
				Name:    "listen-addr",
				Value:   ":8080",
				EnvVars: []string{"VYLET_API_LISTEN_ADDR"},
			},
			&cli.StringFlag{
				Name:    "db-host",
				Value:   "localhost:9090",
				EnvVars: []string{"VYLET_API_DB_HOST"},
			},
			&cli.StringFlag{
				Name:    "cdn-host",
				EnvVars: []string{"VYLET_API_CDN_HOST"},
				Value:   "http://localhost:9525",
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(cmd *cli.Context) error {
	ctx := context.Background()

	logger := telemetry.StartLogger(cmd)
	telemetry.StartMetrics(cmd)

	server, err := server.New(&server.Args{
		Logger:  logger,
		Addr:    cmd.String("listen-addr"),
		DbHost:  cmd.String("db-host"),
		CdnHost: cmd.String("cdn-host"),
	})
	if err != nil {
		return fmt.Errorf("failed to create new server: %w", err)
	}

	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("failed to run server: %w", err)
	}

	return nil
}
