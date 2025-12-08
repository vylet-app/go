package cdn

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/consumer"
	vyletkafka "github.com/vylet-app/go/bus/proto"
	"github.com/vylet-app/go/database/client"
)

type Server struct {
	logger *slog.Logger

	consumer *consumer.Consumer[*vyletkafka.FirehoseEvent]
	db       *client.Client
}

type Args struct {
	Logger *slog.Logger

	BootstrapServers []string
	InputTopic       string
	ConsumerGroup    string

	DatabaseHost string
}

func New(args *Args) (*Server, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	logger := args.Logger

	db, err := client.New(&client.Args{
		Addr: args.DatabaseHost,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create a new database client: %w", err)
	}

	server := Server{
		logger: logger,

		db: db,
	}

	busConsumer, err := consumer.New(
		logger.With("component", "consumer"),
		args.BootstrapServers,
		args.InputTopic,
		args.ConsumerGroup,
		consumer.WithOffset[*vyletkafka.FirehoseEvent](consumer.OffsetStart),
		consumer.WithMessageHandler(server.handleEvent),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new consumer: %w", err)
	}
	server.consumer = busConsumer

	return &server, nil
}

func (s *Server) Run(ctx context.Context) error {
	logger := s.logger.With("name", "Run")

	shutdownConsumer := make(chan struct{}, 1)
	consumerShutdown := make(chan struct{}, 1)
	consumerErr := make(chan error, 1)
	go func() {
		go func() {
			if err := s.consumer.Consume(ctx); err != nil {
				consumerErr <- err
			}
		}()

		select {
		case <-shutdownConsumer:
		case err := <-consumerErr:
			s.logger.Error("error consuming", "err", err)
		}

		s.consumer.Close()

		close(consumerShutdown)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
		close(shutdownConsumer)
	case <-ctx.Done():
		logger.Info("context cancelled")
		close(shutdownConsumer)
	case <-consumerShutdown:
		logger.Warn("consumer shut down unexpectedly")
	}

	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.consumer.Close()

	if err := s.db.Close(); err != nil {
		logger.Error("failed to close database client", "err", err)
	}

	return nil
}
