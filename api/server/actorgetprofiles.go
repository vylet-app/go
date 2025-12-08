package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
	"github.com/vylet-app/go/handlers"
)

func (s *Server) getProfiles(ctx context.Context, dids []string) (map[string]*vylet.ActorDefs_ProfileView, error) {
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

	profiles := make(map[string]*vylet.ActorDefs_ProfileView)
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
			profiles[profile.Did] = &vylet.ActorDefs_ProfileView{
				Did:         profile.Did,
				Handle:      handle,
				Avatar:      profile.Avatar,
				Description: profile.Description,
				DisplayName: profile.DisplayName,
				Pronouns:    profile.Pronouns,
				CreatedAt:   profile.CreatedAt.AsTime().Format(time.RFC3339Nano),
				IndexedAt:   profile.IndexedAt.AsTime().Format(time.RFC3339Nano),
				Viewer: &vylet.ActorDefs_ViewerState{
					BlockedBy:  new(bool),
					Blocking:   new(string),
					FollowedBy: new(string),
					Following:  new(string),
					Muted:      new(bool),
				},
			}
		})
	}
	wg.Wait()

	return profiles, nil
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
				Viewer: &vylet.ActorDefs_ViewerState{
					BlockedBy:  new(bool),
					Blocking:   new(string),
					FollowedBy: new(string),
					Following:  new(string),
					Muted:      new(bool),
				},
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
