package kafkafirehose

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/producer"
)

type KafkaFirehose struct {
	logger *slog.Logger

	consumer *Consumer

	desiredCollections []string
}

type Args struct {
	Logger *slog.Logger

	DesiredCollections []string
	WebsocketHost      string
	BootstrapServers   []string
	ConsumerGroup      string
	OutputTopic        string
}

func New(ctx context.Context, args *Args) (*KafkaFirehose, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	logger := args.Logger

	// create a new consumer
	consumer, err := NewConsumer(&ConsumerArgs{
		Logger: logger.With("component", "consumer"),

		WebsocketHost: "",
	})
	// if creation fails, exit early
	if err != nil {
		return nil, fmt.Errorf("failed to create a new consumer: %w", err)
	}

	busProducer, err := producer.New(ctx, logger.With("component", "producer"), args.BootstrapServers, args.OutputTopic)

	kf := KafkaFirehose{
		logger: args.Logger,

		consumer: consumer,

		desiredCollections: args.DesiredCollections,
	}

	return &kf, nil
}

func (kf *KafkaFirehose) Run(ctx context.Context) error {
	logger := kf.logger.With("name", "Run")

	// run the consumer in a goroutine and
	shutdownConsumer := make(chan struct{}, 1)
	consumerShutdown := make(chan struct{}, 1)
	go func() {
		logger := kf.logger.With("component", "consumer")

		// run the consumer and wait for it to be shut down
		go func() {
			if err := kf.consumer.Run(ctx); err != nil {
				logger.Error("error running consumer", "err", err)
			}
			close(shutdownConsumer)
		}()

		<-shutdownConsumer

		// notify that the consumer has been shut down
		close(consumerShutdown)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	// wait for any of the following to arise
	select {
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
	case <-ctx.Done():
		logger.Info("main context cancelled")
	case <-consumerShutdown:
		logger.Warn("consumer shutdown unexpectedly, forcing exit")
	}

	// create a new context for cleanup with a 10 second timeout
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := consumer.Close(cleanupCtx); err != nil {
		logger.Error("failed to shut down consumer", "err", err)
	}

	logger.Info("kafka firehose shutdown successfully")

	return nil
}
