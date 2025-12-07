package server

import (
	"errors"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
)

type ActorGetProfileInput struct {
	Actor string `query:"actor"`
}

func (s *Server) handleGetProfile(e echo.Context) error {
	ctx := e.Request().Context()
	logger := s.logger.With("name", "handleActorGetProfile")

	var input ActorGetProfileInput
	if err := e.Bind(&input); err != nil {
		return ErrInternalServerErr
	}

	if input.Actor == "" {
		return NewValidationError("actor", "actor parameter is required")
	}

	logger = logger.With("actor", input.Actor)

	did, handle, err := s.fetchDidHandleFromActor(ctx, input.Actor)
	if err != nil {
		if errors.Is(err, ErrActorNotValid) {
			return NewValidationError("actor", "actor must be a valid DID or handle")
		}
		logger.Error("error fetching did and handle", "err", err)
		return ErrInternalServerErr
	}

	resp, err := s.client.Profile.GetProfile(ctx, &vyletdatabase.GetProfileRequest{
		Did: did,
	})
	if err != nil {
		logger.Error("error getting profile", "err", err)
		return ErrInternalServerErr
	}
	if resp.Error != nil {
		if client.IsNotFoundError(resp.Error) {
			return ErrNotFound
		}

		logger.Error("error getting profile", "err", *resp.Error)
		return ErrInternalServerErr
	}

	pv := vylet.ActorDefs_ProfileView{
		Did:         did,
		Handle:      handle,
		Avatar:      resp.Profile.Avatar,
		Description: resp.Profile.Description,
		DisplayName: resp.Profile.DisplayName,
		Pronouns:    resp.Profile.Pronouns,
		CreatedAt:   resp.Profile.CreatedAt.AsTime().Format(time.RFC3339Nano),
		IndexedAt:   resp.Profile.IndexedAt.AsTime().Format(time.RFC3339Nano),
		Viewer: &vylet.ActorDefs_ViewerState{
			BlockedBy:  new(bool),
			Blocking:   new(string),
			FollowedBy: new(string),
			Following:  new(string),
			Muted:      new(bool),
		},
	}

	return e.JSON(200, pv)
}
