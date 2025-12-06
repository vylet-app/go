package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:   "kafka-firehose",
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(cmd *cli.Context) error {
	return nil
}
