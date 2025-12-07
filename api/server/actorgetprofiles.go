package server

import (
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
)

type ActorGetProfilesInput struct {
	Dids []string `query:"dids"`
}

func (s *Server) handleGetProfiles(e echo.Context) error {
	ctx := e.Request().Context()
	logger := s.logger.With("name", "handleActorGetProfiles")

	var input ActorGetProfilesInput
	if err := e.Bind(&input); err != nil {
		return ErrInternalServerErr
	}

	if len(input.Dids) == 0 {
		return NewValidationError("dids", "at least one DID is required")
	}

	if len(input.Dids) > 25 {
		return NewValidationError("dids", "at most 25 DIDs may be supplied")
	}

	logger = logger.With("dids", input.Dids)

	resp, err := s.client.Profile.GetProfiles(ctx, &vyletdatabase.GetProfilesRequest{
		Dids: input.Dids,
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

	profiles := make([]vylet.ActorDefs_ProfileView, 0, len(resp.Profiles))
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
			profiles = append(profiles, vylet.ActorDefs_ProfileView{
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
			})
		})
	}
	wg.Wait()

	return e.JSON(200, profiles)
}
