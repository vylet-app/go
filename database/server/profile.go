package server

import (
	"context"
	"time"

	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) CreateProfile(ctx context.Context, req *vyletdatabase.CreateProfileRequest) (*vyletdatabase.CreateProfileResponse, error) {
	logger := s.logger.With("name", "CreateProfile")
	now := time.Now().UTC()

	if err := s.cqlSession.Query(
		`
		INSERT INTO profiles
			(did, display_name, description, pronouns, avatar, created_at, indexed_at, updated_at)
		VALUES
			(?, ?, ?, ?, ?, ?, ?, ?)
		`,
		req.Profile.Did,
		req.Profile.DisplayName,
		req.Profile.Description,
		req.Profile.Pronouns,
		req.Profile.Avatar,
		req.Profile.CreatedAt.AsTime(),
		now,
		now,
	).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to create profile", "did", req.Profile.Did, "err", err)
		return &vyletdatabase.CreateProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreateProfileResponse{}, nil
}

func (s *Server) UpdateProfile(ctx context.Context, req *vyletdatabase.CreateProfileRequest) (*vyletdatabase.CreateProfileResponse, error) {
	logger := s.logger.With("name", "UpdateProfile")

	now := time.Now().UTC()

	if err := s.cqlSession.Query(
		`
		UPDATE profiles
		SET
			display_name = ?,
			description = ?,
			pronouns = ?,
			avatar = ?,
			updated_at = ?
		WHERE
			did = ?
		`,
		req.Profile.DisplayName,
		req.Profile.Description,
		req.Profile.Pronouns,
		req.Profile.Avatar,
		now,
		req.Profile.Did,
	).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to create profile", "did", req.Profile.Did, "err", err)
		return &vyletdatabase.CreateProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreateProfileResponse{}, nil
}

func (s *Server) DeleteProfile(ctx context.Context, req *vyletdatabase.DeleteProfileRequest) (*vyletdatabase.DeleteProfileResponse, error) {
	logger := s.logger.With("name", "DeleteProfile")

	if err := s.cqlSession.Query(
		`
		DELETE FROM profiles
		WHERE
			did = ?
		`,
		req.Did,
	).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to delete profile", "did", req.Did, "err", err)
		return &vyletdatabase.DeleteProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.DeleteProfileResponse{}, nil
}

func (s *Server) GetProfile(ctx context.Context, req *vyletdatabase.GetProfileRequest) (*vyletdatabase.GetProfileResponse, error) {
	logger := s.logger.With("name", "GetProfile")

	resp := &vyletdatabase.GetProfileResponse{
		Profile: &vyletdatabase.Profile{},
	}
	var createdAt, indexedAt time.Time

	if err := s.cqlSession.Query(
		`SELECT
			did,
			display_name,
			description,
			pronouns,
			avatar,
			created_at,
			indexed_at
		FROM profiles
		WHERE
			did = ?
		`,
		req.Did,
	).WithContext(ctx).Scan(
		&resp.Profile.Did,
		&resp.Profile.DisplayName,
		&resp.Profile.Description,
		&resp.Profile.Pronouns,
		&resp.Profile.Avatar,
		&createdAt,
		&indexedAt,
	); err != nil {
		logger.Error("failed to get profile", "did", req.Did, "err", err)
		return &vyletdatabase.GetProfileResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	resp.Profile.CreatedAt = timestamppb.New(createdAt)
	resp.Profile.IndexedAt = timestamppb.New(indexedAt)

	return resp, nil
}

func (s *Server) GetProfiles(ctx context.Context, req *vyletdatabase.GetProfilesRequest) (*vyletdatabase.GetProfilesResponse, error) {
	logger := s.logger.With("name", "GetProfiles")

	resp := &vyletdatabase.GetProfilesResponse{
		Profiles: make(map[string]*vyletdatabase.Profile),
	}

	iter := s.cqlSession.Query(
		`SELECT
			did,
			display_name,
			description,
			pronouns,
			avatar,
			created_at,
			indexed_at
		FROM profiles
		WHERE
			did IN ?
		`,
		req.Dids,
	).WithContext(ctx).Iter()

	var createdAt, indexedAt time.Time
	for {
		profile := &vyletdatabase.Profile{}

		if !iter.Scan(
			&profile.Did,
			&profile.DisplayName,
			&profile.Description,
			&profile.Pronouns,
			&profile.Avatar,
			&createdAt,
			&indexedAt,
		) {
			break
		}

		profile.CreatedAt = timestamppb.New(createdAt)
		profile.IndexedAt = timestamppb.New(indexedAt)

		resp.Profiles[profile.Did] = profile
	}

	if err := iter.Close(); err != nil {
		logger.Error("failed to get profiles", "dids", req.Dids, "err", err)
		return &vyletdatabase.GetProfilesResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return resp, nil
}

func (s *Server) GetProfileCounts(ctx context.Context, req *vyletdatabase.GetProfileCountsRequest) (*vyletdatabase.GetProfileCountsResponse, error) {
	logger := s.logger.With("name", "GetProfileCounts")

	allCounts := make(map[string]*vyletdatabase.ProfileCounts)

	iter := s.cqlSession.Query(
		`SELECT
			did,
			post_count
		FROM
			profile_counts
		WHERE
			did IN ?
		`,
		req.Dids,
	).WithContext(ctx).Iter()

	for {
		var did string
		counts := &vyletdatabase.ProfileCounts{}
		if !iter.Scan(
			&did,
			&counts.Posts,
		) {
			break
		}

		allCounts[did] = counts
	}

	if err := iter.Close(); err != nil {
		logger.Error("failed to get profile counts", "dids", req.Dids, "err", err)
		return &vyletdatabase.GetProfileCountsResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.GetProfileCountsResponse{
		Counts: allCounts,
	}, nil
}
