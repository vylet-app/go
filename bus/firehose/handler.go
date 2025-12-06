package kafkafirehose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/twmb/franz-go/pkg/kgo"
	vyletkafka "github.com/vylet-app/go/bus/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (kf *KafkaFirehose) handleEvent(ctx context.Context, evt *events.XRPCStreamEvent) error {
	logger := kf.logger.With("name", "handleEvent", "seq", evt.Sequence())
	logger.Debug("received event")

	var kind string
	if evt.RepoIdentity != nil {
		kind = "identity"
	} else if evt.RepoAccount != nil {
		kind = "account"
	} else if evt.RepoCommit != nil {
		kind = "commit"
	} else if evt.RepoInfo != nil {
		kind = "info"
	} else if evt.RepoSync != nil {
		kind = "sync"
	} else {
		kind = "unknown"
	}

	eventsReceived.WithLabelValues(kind).Inc()

	if evt.RepoCommit == nil && evt.RepoIdentity == nil && evt.RepoAccount == nil {
		logger.Debug("not a handled operation, skipping")
		return nil
	}

	kf.setCursor(evt.Sequence())

	var kafkaEvts []*vyletkafka.FirehoseEvent

	if evt.RepoIdentity != nil {
		b, err := json.Marshal(evt.RepoIdentity)
		if err != nil {
			return fmt.Errorf("failed to marshal identity event into bytes: %w", err)
		}

		parsedTime, err := time.Parse(time.RFC3339Nano, evt.RepoIdentity.Time)
		if err != nil {
			return fmt.Errorf("failed to marshal identity event time %s to go time: %w", evt.RepoIdentity.Time, err)
		}

		kafkaEvts = append(kafkaEvts, &vyletkafka.FirehoseEvent{
			Did:       evt.RepoIdentity.Did,
			Timestamp: timestamppb.New(parsedTime),
			Identity:  b,
		})
	} else if evt.RepoAccount != nil {
		b, err := json.Marshal(evt.RepoAccount)
		if err != nil {
			return fmt.Errorf("failed to marshal account event into bytes: %w", err)
		}

		parsedTime, err := time.Parse(time.RFC3339Nano, evt.RepoAccount.Time)
		if err != nil {
			return fmt.Errorf("failed to marshal account event time %s to go time: %w", evt.RepoAccount.Time, err)
		}

		kafkaEvts = append(kafkaEvts, &vyletkafka.FirehoseEvent{
			Did:       evt.RepoAccount.Did,
			Timestamp: timestamppb.New(parsedTime),
			Account:   b,
		})
	} else {
		rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.RepoCommit.Blocks))
		if err != nil {
			logger.Error("failed to read repo from car", "did", evt.RepoCommit.Repo, "err", err)
			return nil
		}

		parsedTime, err := time.Parse(time.RFC3339Nano, evt.RepoCommit.Time)
		if err != nil {
			return fmt.Errorf("failed to marshal commit event time %s to go time: %w", evt.RepoCommit.Time, err)
		}

		protoTime := timestamppb.New(parsedTime)

		for _, op := range evt.RepoCommit.Ops {
			func() {
				status := "error"
				var collection string

				defer func() {
					recordsHandled.WithLabelValues(status, collection).Inc()
				}()

				pts := strings.Split(op.Path, "/")
				if len(pts) != 2 {
					logger.Error("failed to parse path, lenght of parts is not two", "path", op.Path)
					return
				}

				collection = pts[0]
				rkey := pts[1]

				wantsCollection := false
				for _, desiredCollection := range kf.desiredCollections {
					if collection == desiredCollection || strings.HasPrefix(collection, desiredCollection) {
						wantsCollection = true
						break
					}
				}
				if !wantsCollection {
					logger.Debug("collection undesired, skipping", "collection", collection)
					status = "skipped"
					return
				}

				var operation vyletkafka.CommitOperation

				switch op.Action {
				case "create":
					operation = vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE
				case "update":
					operation = vyletkafka.CommitOperation_COMMIT_OPERATION_UPDATE
				case "delete":
					operation = vyletkafka.CommitOperation_COMMIT_OPERATION_DELETE
				}

				var rec map[string]any
				var recCid string

				if op.Action == "create" || op.Action == "update" {
					rcid, recB, err := rr.GetRecordBytes(ctx, op.Path)
					if err != nil {
						logger.Error("failed to read record bytes", "err", err)
						return
					}

					recCid = rcid.String()
					if recCid != op.Cid.String() {
						logger.Error("record cid mismatch", "expected", *op.Cid, "actual", recCid)
						return
					}

					maybeRec, err := atdata.UnmarshalCBOR(*recB)
					if err != nil {
						logger.Error("failed to unmarshal record", "err", err)
					}
					rec = maybeRec
				}

				var b []byte
				if rec != nil {
					maybeB, err := json.Marshal(rec)
					if err != nil {
						logger.Error("failed to marshal record map to json", "err", err)
						return
					}
					b = maybeB
				}

				kafkaEvts = append(kafkaEvts,
					&vyletkafka.FirehoseEvent{
						Did:       evt.RepoCommit.Repo,
						Timestamp: protoTime,
						Commit: &vyletkafka.Commit{
							Rev:        evt.RepoCommit.Rev,
							Operation:  operation,
							Collection: collection,
							Rkey:       rkey,
							Record:     b,
							Cid:        recCid,
						},
					})

				status = "ok"
			}()
		}
	}

	for _, kafkaEvt := range kafkaEvts {
		if err := kf.producer.ProduceAsync(ctx, kafkaEvt.Did, kafkaEvt, func(r *kgo.Record, err error) {
			status := "error"
			defer func() {
				messagesProduced.WithLabelValues(status).Inc()
			}()

			if err != nil {
				logger.Error("error after producting event async", "err", err)
				return
			}

			status = "ok"
			logger.Debug("produced event")
		}); err != nil {
			logger.Error("failed to produce event async", "err", err)
		}
	}

	return nil
}
