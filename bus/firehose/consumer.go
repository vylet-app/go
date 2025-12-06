package kafkafirehose

import (
	"context"
	"log/slog"
)

type Consumer struct {
	logger *slog.Logger

	websocketHost string
}

type ConsumerArgs struct {
	Logger *slog.Logger

	WebsocketHost string
}

func NewConsumer(args *ConsumerArgs) (*Consumer, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	consumer := Consumer{
		logger:        args.Logger,
		websocketHost: args.WebsocketHost,
	}

	return &consumer, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	logger := c.logger.With("name", "Run")
	_ = logger

	return nil
}

func (c *Consumer) Close(ctx context.Context) error {
	return nil
}
