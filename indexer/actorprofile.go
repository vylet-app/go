package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	vyletkafka "github.com/vylet-app/go/bus/proto"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
	"github.com/vylet-app/go/internal/helpers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) handleActorProfile(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	var rec vylet.ActorProfile
	op := evt.Commit
	switch op.Operation {
	case vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE:
		if err := json.Unmarshal(op.Record, &rec); err != nil {
			return fmt.Errorf("failed to unmarshal profile record: %w", err)
		}

		createdAtTime, err := time.Parse(time.RFC3339Nano, rec.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to parse time in record: %w", err)
		}

		req := vyletdatabase.CreateProfileRequest{
			Profile: &vyletdatabase.Profile{
				Did:         evt.Did,
				DisplayName: rec.DisplayName,
				Description: rec.Description,
				Pronouns:    rec.Pronouns,
				CreatedAt:   timestamppb.New(createdAtTime),
			},
		}

		if rec.Avatar != nil {
			req.Profile.Avatar = helpers.ToStringPtr(rec.Avatar.Ref.String())
		}

		resp, err := s.db.Profile.CreateProfile(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to create create profile request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error creating profile: %s", *resp.Error)
		}
	case vyletkafka.CommitOperation_COMMIT_OPERATION_UPDATE:
		if err := json.Unmarshal(op.Record, &rec); err != nil {
			return fmt.Errorf("failed to unmarshal profile record: %w", err)
		}

		req := vyletdatabase.CreateProfileRequest{
			Profile: &vyletdatabase.Profile{
				Did:         evt.Did,
				DisplayName: rec.DisplayName,
				Description: rec.Description,
				Pronouns:    rec.Pronouns,
			},
		}

		if rec.Avatar != nil {
			req.Profile.Avatar = helpers.ToStringPtr(rec.Avatar.Ref.String())
		}

		resp, err := s.db.Profile.UpdateProfile(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to create update profile request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error updating profile: %w", err)
		}
	case vyletkafka.CommitOperation_COMMIT_OPERATION_DELETE:
		resp, err := s.db.Profile.DeleteProfile(ctx, &vyletdatabase.DeleteProfileRequest{
			Did: evt.Did,
		})
		if err != nil {
			return fmt.Errorf("failed to create delete profile request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error deleting profile: %s", *resp.Error)
		}
	}

	return nil
}
