package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
)

type ActorGetProfileInput struct {
	Actor string `query:"actor"`
}

func (s *Server) getProfile(ctx context.Context, actor string) (*vylet.ActorDefs_ProfileView, error) {
	did, handle, err := s.fetchDidHandleFromActor(ctx, actor)
	if err != nil {
		return nil, fmt.Errorf("error fetching did and handle: %w", err)
	}

	resp, err := s.client.Profile.GetProfile(ctx, &vyletdatabase.GetProfileRequest{
		Did: did,
	})
	if err != nil {
		return nil, fmt.Errorf("error getting profile: %w", err)
	}
	if resp.Error != nil {
		if client.IsNotFoundError(resp.Error) {
			return nil, ErrDatabaseNotFound
		}
		return nil, fmt.Errorf("error getting profile: %s", *resp.Error)
	}

	return &vylet.ActorDefs_ProfileView{
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
	}, nil
}

func (s *Server) getProfileBasic(ctx context.Context, actor string) (*vylet.ActorDefs_ProfileViewBasic, error) {
	did, handle, err := s.fetchDidHandleFromActor(ctx, actor)
	if err != nil {
		return nil, fmt.Errorf("error fetching did and handle: %w", err)
	}

	resp, err := s.client.Profile.GetProfile(ctx, &vyletdatabase.GetProfileRequest{
		Did: did,
	})
	if err != nil {
		return nil, fmt.Errorf("error getting profile: %w", err)
	}
	if resp.Error != nil {
		if client.IsNotFoundError(resp.Error) {
			return nil, ErrDatabaseNotFound
		}
		return nil, fmt.Errorf("error getting profile: %s", *resp.Error)
	}

	return &vylet.ActorDefs_ProfileViewBasic{
		Did:         did,
		Handle:      handle,
		Avatar:      resp.Profile.Avatar,
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
	}, nil
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

	profile, err := s.getProfile(ctx, input.Actor)
	if err != nil {
		if errors.Is(err, ErrActorNotValid) {
			return NewValidationError("actor", "actor parameter must be a valid DID or handle")
		}
		if errors.Is(err, ErrDatabaseNotFound) {
			return ErrNotFound
		}
		logger.Error("error getting profile", "err", err)
		return ErrInternalServerErr
	}

	return e.JSON(200, profile)
}
