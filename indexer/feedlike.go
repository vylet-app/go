package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	vyletkafka "github.com/vylet-app/go/bus/proto"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) handleFeedLike(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	var rec vylet.FeedLike
	op := evt.Commit
	uri := firehoseEventToUri(evt)
	switch op.Operation {
	case vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE:
		if err := json.Unmarshal(op.Record, &rec); err != nil {
			return fmt.Errorf("failed to unmarshal like record: %w", err)
		}

		createdAtTime, err := time.Parse(time.RFC3339Nano, rec.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to parse time from record: %w", err)
		}

		req := vyletdatabase.CreateLikeRequest{
			Like: &vyletdatabase.Like{
				Uri:        uri,
				Cid:        evt.Commit.Cid,
				AuthorDid:  evt.Did,
				CreatedAt:  timestamppb.New(createdAtTime),
				SubjectUri: rec.Subject.Uri,
				SubjectCid: rec.Subject.Cid,
			},
		}

		resp, err := s.db.Like.CreateLike(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to create create like request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error creating like: %s", *resp.Error)
		}
	case vyletkafka.CommitOperation_COMMIT_OPERATION_UPDATE:
		return fmt.Errorf("unsupported like update event")
	case vyletkafka.CommitOperation_COMMIT_OPERATION_DELETE:
		resp, err := s.db.Like.DeleteLike(ctx, &vyletdatabase.DeleteLikeRequest{
			Uri: uri,
		})
		if err != nil {
			return fmt.Errorf("failed to create delete like request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error deleting like %s", *resp.Error)
		}
	}

	return nil
}
