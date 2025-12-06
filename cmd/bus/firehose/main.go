package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bluesky-social/go-util/pkg/telemetry"
	"github.com/urfave/cli/v2"
	kafkafirehose "github.com/vylet-app/go/bus/firehose"
)

func main() {
	app := cli.App{
		Name: "kafka-firehose",
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			&cli.StringSliceFlag{
				Name:    "desired-collections",
				EnvVars: []string{"KAFKA_FIREHOSE_DESIRED_COLLECTIONS"},
			},
			&cli.StringFlag{
				Name:    "websocket-host",
				EnvVars: []string{"KAFKA_FIREHOSE_WEBSOCKET_HOST"},
				Value:   "wss://bsky.network",
			},
			&cli.StringSliceFlag{
				Name:    "bootstrap-servers",
				EnvVars: []string{"KAFKA_FIREHOSE_BOOTSTRAP_SERVERS"},
				Value:   cli.NewStringSlice("localhost:9092"),
			},
			&cli.StringFlag{
				Name:     "consumer-group",
				EnvVars:  []string{"KAFKA_FIREHOSE_CONSUMER_GROUP"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "output-topic",
				EnvVars: []string{"KAFKA_FIREHOSE_OUTPUT_TOPIC"},
				Value:   "firehose-records-prod",
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

	kf, err := kafkafirehose.New(&kafkafirehose.Args{
		Logger: logger,

		DesiredCollections: cmd.StringSlice("desired-collections"),
	})
	if err != nil {
		return fmt.Errorf("failed to create new kafka firehose: %w", err)
	}

	if err := kf.Run(ctx); err != nil {
		return err
	}

	return nil
}
