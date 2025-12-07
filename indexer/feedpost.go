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

func (s *Server) handleFeedPost(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	var rec vylet.FeedPost
	op := evt.Commit
	uri := firehoseEventToUri(evt)
	switch op.Operation {
	case vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE:
		if err := json.Unmarshal(op.Record, &rec); err != nil {
			return fmt.Errorf("failed to unmarshal post record: %w", err)
		}

		createdAtTime, err := time.Parse(time.RFC3339Nano, rec.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to parse time from record: %w", err)
		}

		var images []*vyletdatabase.Image
		if rec.Media == nil || rec.Media.MediaImages == nil || len(rec.Media.MediaImages.Images) == 0 {
			return fmt.Errorf("invalid post, missing or empty images")
		}

		for _, img := range rec.Media.MediaImages.Images {
			dbimg := &vyletdatabase.Image{
				Cid: img.Image.Ref.String(),
				Alt: &img.Alt,
			}
			if img.AspectRatio != nil {
				dbimg.Width = &img.AspectRatio.Width
				dbimg.Height = &img.AspectRatio.Height
			}
			images = append(images, dbimg)
		}

		req := vyletdatabase.CreatePostRequest{
			Post: &vyletdatabase.Post{
				Uri:       uri,
				Images:    images,
				Caption:   rec.Caption,
				CreatedAt: timestamppb.New(createdAtTime),
			},
		}

		if rec.Facets != nil {
			b, err := json.Marshal(rec.Facets)
			if err != nil {
				return fmt.Errorf("failed to marshal facets: %w", err)
			}
			req.Post.Facets = b
		}

		resp, err := s.db.Post.CreatePost(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to create create post request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error creating post: %s", *resp.Error)
		}
	case vyletkafka.CommitOperation_COMMIT_OPERATION_UPDATE:
		return fmt.Errorf("unsupported post update event")
	case vyletkafka.CommitOperation_COMMIT_OPERATION_DELETE:
		resp, err := s.db.Post.DeletePost(ctx, &vyletdatabase.DeletePostRequest{
			Uri: uri,
		})
		if err != nil {
			return fmt.Errorf("failed to create delete post request: %w", err)
		}
		if resp.Error != nil {
			return fmt.Errorf("error deleting post %s", *resp.Error)
		}
	}

	return nil
}
