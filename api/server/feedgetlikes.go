package server

import (
	"context"
	"fmt"
	"time"

	"github.com/labstack/echo/v4"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
)

type GetFeedLikesBySubjectInput struct {
	Uri    string  `query:"uri"`
	Limit  int64   `query:"limit"`
	Cursor *string `query:"cursor"`
}

func (s *Server) getLikesBySubject(ctx context.Context, subjectUri string, limit int64, cursor *string) ([]*vylet.FeedGetSubjectLikes_Like, error) {
	logger := s.logger.With("name", "getLikesBySubject", "uri", subjectUri)

	resp, err := s.client.Like.GetLikesBySubject(ctx, &vyletdatabase.GetLikesBySubjectRequest{
		SubjectUri: subjectUri,
		Limit:      limit,
		Cursor:     cursor,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get likes by subject: %w", err)
	}

	dids := make([]string, 0, len(resp.Likes))
	for _, like := range resp.Likes {
		dids = append(dids, like.AuthorDid)
	}

	profiles, err := s.getProfiles(ctx, dids)
	if err != nil {
		return nil, fmt.Errorf("failed to get profiles for subject: %w", err)
	}

	likes := make([]*vylet.FeedGetSubjectLikes_Like, 0, len(resp.Likes))
	for _, like := range resp.Likes {
		profile, ok := profiles[like.AuthorDid]
		if !ok {
			logger.Warn("failed to find profile for like", "did", like.AuthorDid, "uri", like.Uri)
			continue
		}

		likes = append(likes, &vylet.FeedGetSubjectLikes_Like{
			Actor:     profile,
			CreatedAt: like.CreatedAt.AsTime().Format(time.RFC3339Nano),
			IndexedAt: like.IndexedAt.AsTime().Format(time.RFC3339Nano),
		})
	}

	return likes, nil
}

func (s *Server) handleGetLikesBySubject(e echo.Context) error {
	ctx := e.Request().Context()

	logger := s.logger.With("name", "handleGetLikesByPost")

	var input GetFeedLikesBySubjectInput
	if err := e.Bind(&input); err != nil {
		logger.Error("failed to bind request", "err", err)
		return ErrInternalServerErr
	}

	if input.Limit < 1 || input.Limit > 100 {
		return NewValidationError("limit", "limit must be between 1 and 100")
	}

	logger = logger.With("uri", input.Uri)

	likes, err := s.getLikesBySubject(ctx, input.Uri, input.Limit, input.Cursor)
	if err != nil {
		logger.Error("failed to get subject likes", "err", err)
		return ErrInternalServerErr
	}

	return e.JSON(200, vylet.FeedGetSubjectLikes_Output{
		Likes: likes,
	})
}
