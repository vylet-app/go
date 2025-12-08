package server

import (
	"context"
	"fmt"
	"time"

	"github.com/labstack/echo/v4"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
	"github.com/vylet-app/go/handlers"
	"github.com/vylet-app/go/internal/helpers"
)

func (s *Server) getLikesBySubject(ctx context.Context, subjectUri string, limit int64, cursor *string) ([]*vylet.FeedGetSubjectLikes_Like, *string, error) {
	logger := s.logger.With("name", "getLikesBySubject", "uri", subjectUri)

	resp, err := s.client.Like.GetLikesBySubject(ctx, &vyletdatabase.GetLikesBySubjectRequest{
		SubjectUri: subjectUri,
		Limit:      limit,
		Cursor:     cursor,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get likes by subject: %w", err)
	}

	dids := make([]string, 0, len(resp.Likes))
	for _, like := range resp.Likes {
		dids = append(dids, like.AuthorDid)
	}

	profiles, err := s.getProfiles(ctx, dids)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get profiles for subject: %w", err)
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

	return likes, resp.Cursor, nil
}

func (s *Server) FeedGetSubjectLikesRequiresAuth() bool {
	return false
}

func (s *Server) HandleFeedGetSubjectLikes(e echo.Context, input *handlers.FeedGetSubjectLikesInput) (*vylet.FeedGetSubjectLikes_Output, *echo.HTTPError) {
	ctx := e.Request().Context()

	logger := s.logger.With("name", "HandleFeedGetSubjectLikes")

	if input.Uri == "" {
		return nil, NewValidationError("uri", "URI must be provided")
	}

	if input.Limit != nil && (*input.Limit < 1 || *input.Limit > 100) {
		return nil, NewValidationError("limit", "limit must be between 1 and 100")
	} else if input.Limit == nil {
		input.Limit = helpers.ToInt64Ptr(25)
	}

	logger = logger.With("uri", input.Uri)

	likes, cursor, err := s.getLikesBySubject(ctx, input.Uri, *input.Limit, input.Cursor)
	if err != nil {
		logger.Error("failed to get subject likes", "err", err)
		return nil, ErrInternalServerErr
	}

	return &vylet.FeedGetSubjectLikes_Output{
		Likes:  likes,
		Cursor: cursor,
	}, nil
}
