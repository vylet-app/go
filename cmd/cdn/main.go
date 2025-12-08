package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
	"github.com/vylet-app/go/cdn"
)

func main() {
	app := cli.App{
		Name: "vylet-cdn",
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			&cli.StringFlag{
				Name:    "database-host",
				Value:   "127.0.0.1:9090",
				EnvVars: []string{"VYLET_CDN_DATABASE_HOST", "VYLET_DATABASE_HOST"},
			},
			&cli.StringSliceFlag{
				Name:    "bootstrap-servers",
				Value:   cli.NewStringSlice("localhost:9092"),
				EnvVars: []string{"VYLET_BOOTSTRAP_SERVERS"},
			},
			&cli.StringFlag{
				Name:    "input-topic",
				Value:   "firehose-events-prod",
				EnvVars: []string{"VYLET_CDN_INPUT_TOPIC"},
			},
			&cli.StringFlag{
				Name:     "consumer-group",
				Required: true,
				EnvVars:  []string{"VYLET_CDN_CONSUMER_GROUP"},
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

	server, err := cdn.New(&cdn.Args{
		Logger:           logger,
		BootstrapServers: cmd.StringSlice("bootstrap-servers"),
		InputTopic:       cmd.String("input-topic"),
		ConsumerGroup:    cmd.String("consumer-group"),
		DatabaseHost:     cmd.String("database-host"),
	})
	if err != nil {
		return fmt.Errorf("failed to create new server: %w", err)
	}

	if err := server.Run(ctx); err != nil {
		return fmt.Errorf("failed to run server: %w", err)
	}

	return nil
}
