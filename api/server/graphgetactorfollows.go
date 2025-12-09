package server

import (
	"errors"

	"github.com/labstack/echo/v4"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/handlers"
	"github.com/vylet-app/go/generated/vylet"
	"github.com/vylet-app/go/internal/helpers"
)

func (s *Server) GraphGetActorFollowsRequiresAuth() bool {
	return false
}

func (s *Server) HandleGraphGetActorFollows(e echo.Context, input *handlers.GraphGetActorFollowsInput) (*vylet.GraphGetActorFollows_Output, *echo.HTTPError) {
	ctx := e.Request().Context()

	logger := s.logger.With("name", "HandleGraphGetActorFollows")

	if input.Limit != nil && (*input.Limit < 1 || *input.Limit > 100) {
		return nil, NewValidationError("limit", "limit must be between 1 and 100")
	} else if input.Limit == nil {
		input.Limit = helpers.ToInt64Ptr(25)
	}

	did, _, err := s.fetchDidHandleFromActor(ctx, input.Actor)
	if err != nil {
		if errors.Is(err, ErrActorNotValid) {
			return nil, NewValidationError("author", "author must be a valid DID or handle")
		}
		logger.Error("error did from actor", "err", err)
		return nil, ErrInternalServerErr
	}

	resp, err := s.client.Follow.GetFollowsByActor(ctx, &vyletdatabase.GetFollowsByActorRequest{
		Did:    did,
		Limit:  *input.Limit,
		Cursor: input.Cursor,
	})
	if err != nil {
		logger.Error("error getting follows", "err", err)
		return nil, ErrInternalServerErr
	}

	dids := make([]string, 0, len(resp.Follows))
	for _, f := range resp.Follows {
		dids = append(dids, f.SubjectDid)
	}

	profiles, err := s.getProfiles(ctx, dids)
	if err != nil {
		logger.Error("error getting profiles", "err", err)
		return nil, ErrInternalServerErr
	}

	sortedProfiles := make([]*vylet.ActorDefs_ProfileView, 0, len(profiles))
	for _, did := range dids {
		profile, ok := profiles[did]
		if !ok {
			logger.Warn("unable to find profile", "did", did)
			continue
		}
		sortedProfiles = append(sortedProfiles, profile)
	}

	return &vylet.GraphGetActorFollows_Output{
		Profiles: sortedProfiles,
		Cursor:   resp.Cursor,
	}, nil
}
