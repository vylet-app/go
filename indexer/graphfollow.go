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

func (s *Server) handleGraphFollow(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	var rec vylet.GraphFollow
	op := evt.Commit
	uri := firehoseEventToUri(evt)
	switch op.Operation {
	case vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE:
		if err := json.Unmarshal(op.Record, &rec); err != nil {
			return fmt.Errorf("failed to unmarshal follow record: %w", err)
		}

		createdAtTime, err := time.Parse(time.RFC3339Nano, rec.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to parse time from record: %w", err)
		}

		req := vyletdatabase.CreateFollowRequest{
			Follow: &vyletdatabase.Follow{
				Uri:        uri,
				Cid:        evt.Commit.Cid,
				SubjectDid: rec.Subject,
				AuthorDid:  evt.Did,
				CreatedAt:  timestamppb.New(createdAtTime),
			},
		}

		resp, err := s.db.Follow.CreateFollow(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to create create follow request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error creating follow: %s", *resp.Error)
		}
	case vyletkafka.CommitOperation_COMMIT_OPERATION_UPDATE:
		return fmt.Errorf("unsupported follow update event")
	case vyletkafka.CommitOperation_COMMIT_OPERATION_DELETE:
		resp, err := s.db.Follow.DeleteFollow(ctx, &vyletdatabase.DeleteFollowRequest{
			Uri: uri,
		})
		if err != nil {
			return fmt.Errorf("failed to create delete follow request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error deleting follow %s", *resp.Error)
		}
	}

	return nil
}
