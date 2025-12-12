package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/handlers"
	"github.com/vylet-app/go/generated/vylet"
	"golang.org/x/sync/errgroup"
)

func (s *Server) getProfiles(ctx context.Context, dids []string) (map[string]*vylet.ActorDefs_ProfileView, error) {
	wg, ctx := errgroup.WithContext(ctx)

	var profiles map[string]*vyletdatabase.Profile
	var profileCounts map[string]*vyletdatabase.ProfileCounts
	var followCounts map[string]*vyletdatabase.FollowCounts

	wg.Go(func() error {
		resp, err := s.client.Profile.GetProfiles(ctx, &vyletdatabase.GetProfilesRequest{
			Dids: dids,
		})
		if err != nil {
			return fmt.Errorf("error getting profiles: %w", err)
		}
		if resp.Error != nil {
			if *resp.Error != "not found" {
				return fmt.Errorf("failed to get profiles: %s", *resp.Error)
			}
		}
		profiles = resp.Profiles
		return nil
	})

	wg.Go(func() error {
		resp, err := s.client.Profile.GetProfileCounts(ctx, &vyletdatabase.GetProfileCountsRequest{
			Dids: dids,
		})
		if err != nil {
			return fmt.Errorf("error getting profile counts: %w", err)
		}
		if resp.Error != nil && !client.IsNotFoundError(resp.Error) {
			return fmt.Errorf("error getting profile counts: %s", *resp.Error)
		}
		profileCounts = resp.Counts
		return nil
	})
	wg.Go(func() error {
		resp, err := s.client.Follow.GetFollowCounts(ctx, &vyletdatabase.GetFollowCountsRequest{
			Dids: dids,
		})
		if err != nil {
			return fmt.Errorf("error getting follow counts: %w", err)
		}
		if resp.Error != nil && !client.IsNotFoundError(resp.Error) {
			return fmt.Errorf("error getting follow counts: %s", *resp.Error)
		}
		followCounts = resp.Counts
		return nil
	})
	if err := wg.Wait(); err != nil {
		return nil, fmt.Errorf("failed to get profiles: %w", err)
	}

	profileViews := make(map[string]*vylet.ActorDefs_ProfileView)
	var lk sync.Mutex
	for _, profile := range profiles {
		wg.Go(func() error {
			_, handle, err := s.fetchDidHandleFromActor(ctx, profile.Did)
			if err != nil {
				s.logger.Error("error getting handle for did", "did", profile.Did, "err", err)
				return nil
			}

			lk.Lock()
			defer lk.Unlock()

			followCounts, ok := followCounts[profile.Did]
			if !ok {
				s.logger.Error("no follow counts for did", "did", profile.Did)
				return nil
			}

			profileCounts, ok := profileCounts[profile.Did]
			if !ok {
				s.logger.Error("no profile counts for did", "did", profile.Did)
				return nil
			}

			profileViews[profile.Did] = &vylet.ActorDefs_ProfileView{
				Did:            profile.Did,
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
			}

			return nil
		})
	}
	wg.Wait()

	return profileViews, nil
}

func (s *Server) getProfilesBasic(ctx context.Context, dids []string) (map[string]*vylet.ActorDefs_ProfileViewBasic, error) {
	resp, err := s.client.Profile.GetProfiles(ctx, &vyletdatabase.GetProfilesRequest{
		Dids: dids,
	})
	if err != nil {
		return nil, fmt.Errorf("error getting profiles: %w", err)
	}
	if resp.Error != nil {
		if *resp.Error != "not found" {
			return nil, fmt.Errorf("failed to get profiles: %s", *resp.Error)
		}
	}

	profiles := make(map[string]*vylet.ActorDefs_ProfileViewBasic)
	var wg sync.WaitGroup
	var lk sync.Mutex
	for _, profile := range resp.Profiles {
		wg.Go(func() {
			_, handle, err := s.fetchDidHandleFromActor(ctx, profile.Did)
			if err != nil {
				s.logger.Error("error getting handle for did", "did", profile.Did, "err", err)
				return
			}

			lk.Lock()
			defer lk.Unlock()
			profiles[profile.Did] = &vylet.ActorDefs_ProfileViewBasic{
				Did:         profile.Did,
				Handle:      handle,
				Avatar:      profile.Avatar,
				DisplayName: profile.DisplayName,
				Pronouns:    profile.Pronouns,
				CreatedAt:   profile.CreatedAt.AsTime().Format(time.RFC3339Nano),
				IndexedAt:   profile.IndexedAt.AsTime().Format(time.RFC3339Nano),
				Viewer:      &vylet.ActorDefs_ViewerState{},
			}
		})
	}
	wg.Wait()

	return profiles, nil
}

func (s *Server) ActorGetProfilesRequiresAuth() bool {
	return false
}

func (s *Server) HandleActorGetProfiles(e echo.Context, input *handlers.ActorGetProfilesInput) (*vylet.ActorGetProfiles_Output, *echo.HTTPError) {
	ctx := e.Request().Context()
	logger := s.logger.With("name", "HandleActorGetProfiles")

	if len(input.Dids) == 0 {
		return nil, NewValidationError("dids", "at least one DID is required")
	}

	if len(input.Dids) > 25 {
		return nil, NewValidationError("dids", "at most 25 DIDs may be supplied")
	}

	logger = logger.With("dids", input.Dids)

	profiles, err := s.getProfiles(ctx, input.Dids)
	if err != nil {
		logger.Error("error getting profiles", "err", err)
		return nil, ErrInternalServerErr
	}

	if len(profiles) == 0 {
		return nil, ErrNotFound
	}

	orderedProfiles := make([]*vylet.ActorDefs_ProfileView, len(profiles))
	for _, did := range input.Dids {
		profile, ok := profiles[did]
		if !ok {
			logger.Warn("did not find profile for specified DID", "did", did)
			continue
		}
		orderedProfiles = append(orderedProfiles, profile)
	}

	return &vylet.ActorGetProfiles_Output{
		Profiles: orderedProfiles,
	}, nil
}
