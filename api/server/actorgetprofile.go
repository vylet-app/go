package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/handlers"
	"github.com/vylet-app/go/generated/vylet"
	"golang.org/x/sync/errgroup"
)

type ActorGetProfileInput struct {
	Actor string `query:"actor"`
}

func (s *Server) getProfile(ctx context.Context, actor string) (*vylet.ActorDefs_ProfileView, error) {
	did, handle, err := s.fetchDidHandleFromActor(ctx, actor)
	if err != nil {
		return nil, fmt.Errorf("error fetching did and handle: %w", err)
	}

	var profile *vyletdatabase.Profile
	var followCounts *vyletdatabase.FollowCounts
	var profileCounts *vyletdatabase.ProfileCounts

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		resp, err := s.client.Profile.GetProfile(ctx, &vyletdatabase.GetProfileRequest{
			Did: did,
		})
		if err != nil {
			return fmt.Errorf("error getting profile: %w", err)
		}
		if resp.Error != nil {
			if client.IsNotFoundError(resp.Error) {
				return ErrDatabaseNotFound
			}
			return fmt.Errorf("error getting profile: %s", *resp.Error)
		}
		profile = resp.Profile
		return nil
	})
	wg.Go(func() error {
		resp, err := s.client.Profile.GetProfileCounts(ctx, &vyletdatabase.GetProfileCountsRequest{
			Dids: []string{did},
		})
		if err != nil {
			return fmt.Errorf("error getting profile counts: %w", err)
		}
		if resp.Error != nil && !client.IsNotFoundError(resp.Error) {
			return fmt.Errorf("error getting profile counts: %s", *resp.Error)
		}
		counts, ok := resp.Counts[did]
		if !ok {
			profileCounts = &vyletdatabase.ProfileCounts{
				Posts: 0,
			}
			return nil
		}
		profileCounts = counts
		return nil
	})
	wg.Go(func() error {
		resp, err := s.client.Follow.GetFollowCounts(ctx, &vyletdatabase.GetFollowCountsRequest{
			Dids: []string{did},
		})
		if err != nil {
			return fmt.Errorf("error getting follow counts: %w", err)
		}
		if resp.Error != nil && !client.IsNotFoundError(resp.Error) {
			return fmt.Errorf("error getting follow counts: %s", *resp.Error)
		}
		counts, ok := resp.Counts[did]
		if !ok {
			followCounts = &vyletdatabase.FollowCounts{
				Following: 0,
				Followers: 0,
			}
			return nil
		}
		followCounts = counts
		return nil
	})
	wg.Wait()

	return &vylet.ActorDefs_ProfileView{
		Did:            did,
		Handle:         handle,
		Avatar:         profile.Avatar,
		Description:    profile.Description,
		DisplayName:    profile.DisplayName,
		Pronouns:       profile.Pronouns,
		PostCount:      profileCounts.Posts,
		FollowingCount: followCounts.Following,
		FollowersCount: followCounts.Followers,
		CreatedAt:      profile.CreatedAt.AsTime().Format(time.RFC3339Nano),
		IndexedAt:      profile.IndexedAt.AsTime().Format(time.RFC3339Nano),
		Viewer:         &vylet.ActorDefs_ViewerState{},
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
		Viewer:      &vylet.ActorDefs_ViewerState{},
	}, nil
}

func (s *Server) ActorGetProfileRequiresAuth() bool {
	return false
}

func (s *Server) HandleActorGetProfile(e echo.Context, input *handlers.ActorGetProfileInput) (*vylet.ActorDefs_ProfileView, *echo.HTTPError) {
	ctx := e.Request().Context()
	logger := s.logger.With("name", "HandleActorGetProfile")

	if input.Actor == "" {
		return nil, NewValidationError("actor", "actor parameter is required")
	}

	logger = logger.With("actor", input.Actor)

	profile, err := s.getProfile(ctx, input.Actor)
	if err != nil {
		if errors.Is(err, ErrActorNotValid) {
			return nil, NewValidationError("actor", "actor parameter must be a valid DID or handle")
		}
		if errors.Is(err, ErrDatabaseNotFound) {
			return nil, ErrNotFound
		}
		logger.Error("error getting profile", "err", err)
		return nil, ErrInternalServerErr
	}

	return profile, nil
}
