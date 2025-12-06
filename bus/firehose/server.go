package kafkafirehose

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bluesky-social/go-util/pkg/bus/cursor"
	"github.com/bluesky-social/go-util/pkg/bus/producer"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"
	vyletkafka "github.com/vylet-app/go/bus/proto"
)

type KafkaFirehose struct {
	logger *slog.Logger

	producer *producer.Producer[*vyletkafka.FirehoseEvent]

	cursor          *cursor.Cursor[*vyletkafka.SequenceCursor]
	lastCursor      *int64
	cursorLk        sync.Mutex
	saveLastCursor  chan struct{}
	lastCursorSaved chan struct{}

	desiredCollections []string
	websocketHost      string
}

type Args struct {
	Logger *slog.Logger

	DesiredCollections []string
	WebsocketHost      string
	BootstrapServers   []string
	OutputTopic        string
}

func New(ctx context.Context, args *Args) (*KafkaFirehose, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	logger := args.Logger

	busProducer, err := producer.New(
		ctx,
		logger.With("component", "producer"),
		args.BootstrapServers,
		args.OutputTopic,
		producer.WithEnsureTopic[*vyletkafka.FirehoseEvent](true),
		producer.WithTopicPartitions[*vyletkafka.FirehoseEvent](24),
		producer.WithRetentionTime[*vyletkafka.FirehoseEvent](24*time.Hour),
		producer.WithReplicationFactor[*vyletkafka.FirehoseEvent](1),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	cursorProducer, err := cursor.New[*vyletkafka.SequenceCursor](ctx, args.BootstrapServers, args.OutputTopic+"-cursor")
	if err != nil {
		return nil, fmt.Errorf("failed to create cursor producer: %w", err)
	}

	desiredCollections := make([]string, len(args.DesiredCollections))
	for idx, coll := range args.DesiredCollections {
		desiredCollections[idx] = strings.TrimSuffix(strings.TrimSuffix(coll, ".*"), ".")
	}

	kf := KafkaFirehose{
		logger: args.Logger,

		producer: busProducer,

		cursor:          cursorProducer,
		saveLastCursor:  make(chan struct{}, 1),
		lastCursorSaved: make(chan struct{}, 1),

		desiredCollections: desiredCollections,
		websocketHost:      args.WebsocketHost,
	}

	logger.Info("attempting to fetch last cursor from bus")
	if err := kf.loadCursor(ctx); err != nil {
		return nil, fmt.Errorf("failed to fetch or init cursor: %w", err)
	}

	return &kf, nil
}

func (kf *KafkaFirehose) Run(ctx context.Context) error {
	logger := kf.logger.With("name", "Run")

	wsDialer := websocket.DefaultDialer
	u, err := url.Parse(kf.websocketHost)
	if err != nil {
		return fmt.Errorf("failed to parse websocket host: %w", err)
	}

	u.Path = "/xrpc/com.atproto.sync.subscribeRepos"

	cursor := kf.getCursor()
	if cursor != nil {
		u.RawQuery = fmt.Sprintf("cursor=%d", *cursor)
	}

	// run the consumer in a goroutine and
	shutdownConsumer := make(chan struct{}, 1)
	consumerShutdown := make(chan struct{}, 1)

	go func() {
		logger := kf.logger.With("component", "consumer")

		logger.Info("subscribing to repo event stream", "url", u.String())

		// dial the websocket
		conn, _, err := wsDialer.Dial(u.String(), http.Header{
			"User-Agent": []string{"vylet-kafka/0.0.0"},
		})
		if err != nil {
			logger.Error("error dialing websocket", "err", err)
			close(shutdownConsumer)
			return
		}

		// setup a new event scheduler
		parallelism := 400

		scheduler := parallel.NewScheduler(parallelism, 1000, kf.websocketHost, kf.handleEvent)

		// run the consumer and wait for it to be shut down
		go func() {
			if err := events.HandleRepoStream(ctx, conn, scheduler, logger); err != nil {
				logger.Error("error handling repo stream", "err", err)
			}
		}()

		<-shutdownConsumer

		if err := conn.Close(); err != nil {
			logger.Error("error closing websocket", "err", err)
		} else {
			logger.Info("websocket closed")
		}

		close(consumerShutdown)
	}()

	go kf.periodicallySaveCursor(ctx)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	// wait for any of the following to arise
	select {
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
		close(shutdownConsumer)
	case <-ctx.Done():
		logger.Info("main context cancelled")
		close(shutdownConsumer)
	case <-consumerShutdown:
		logger.Warn("consumer shutdown unexpectedly, forcing exit")
	}

	select {
	case <-consumerShutdown:
	case <-time.After(5 * time.Second):
		logger.Warn("websocket did not shut down within five seconds, forcefully shutting down")
	}

	// close the producer
	kf.producer.Close()
	if err := kf.cursor.Close(); err != nil {
		logger.Error("error closing cursor", "err", err)
	}

	logger.Info("kafka firehose shutdown successfully")

	return nil
}

func isFinalCursor(c *vyletkafka.SequenceCursor) bool {
	return c != nil && c.SavedOnExit
}

func (kf *KafkaFirehose) loadCursor(ctx context.Context) error {
	kf.cursorLk.Lock()
	defer kf.cursorLk.Unlock()

	if c, err := kf.cursor.Load(ctx, isFinalCursor); err != nil {
		return fmt.Errorf("failed to load cursor: %w", err)
	} else if c != nil {
		kf.lastCursor = &c.Sequence
		kf.logger.Info("loaded last cursor", "cursor", kf.lastCursor)
	} else {
		kf.logger.Info("no previous cursor found, starting fresh")
	}

	return nil
}

func (kf *KafkaFirehose) getCursor() *int64 {
	kf.cursorLk.Lock()
	defer kf.cursorLk.Unlock()

	return kf.lastCursor
}

func (kf *KafkaFirehose) setCursor(c int64) {
	kf.cursorLk.Lock()
	defer kf.cursorLk.Unlock()
	kf.lastCursor = &c
}

func (kf *KafkaFirehose) periodicallySaveCursor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	defer func() {
		kf.cursorLk.Lock()
		defer kf.cursorLk.Unlock()
		if kf.lastCursor != nil {
			finalCursor := vyletkafka.SequenceCursor{Sequence: *kf.lastCursor, SavedOnExit: true}
			if err := kf.cursor.Save(context.Background(), &finalCursor); err != nil {
				kf.logger.Error("failed to save final cursor", "err", err)
			} else {
				kf.logger.Info("saved final cursor on exit", "cursor", *kf.lastCursor)
			}
		}
		close(kf.lastCursorSaved)
	}()

	for {
		select {
		case <-kf.saveLastCursor:
			kf.logger.Info("saving last cursor...")
			return
		case <-ticker.C:
			kf.cursorLk.Lock()
			last := kf.lastCursor
			kf.cursorLk.Unlock()

			if last != nil {
				if err := kf.cursor.Save(ctx, &vyletkafka.SequenceCursor{Sequence: *last}); err != nil {
					kf.logger.Info("failed to save cursor", "err", err)
				} else {
					kf.logger.Info("saved cursor", "sequence", *last)
				}
			}
		}
	}
}
