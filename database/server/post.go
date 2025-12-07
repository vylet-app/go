package server

import (
	"context"

	vyletdatabase "github.com/vylet-app/go/database/proto"
)

func (s *Server) CreatePost(ctx context.Context, req *vyletdatabase.CreatePostRequest) (*vyletdatabase.CreatePostResponse, error) {
	// logger := s.logger.With("name", "CreateProfile")
	// now := time.Now().UTC()
	//
	// if err := s.cqlSession.Query(
	// 	`
	// 	INSERT INTO profiles
	// 		(did, display_name, description, pronouns, avatar, created_at, indexed_at, updated_at)
	// 	VALUES
	// 		(?, ?, ?, ?, ?, ?, ?, ?)
	// 	`,
	// 	req.Profile.Did,
	// 	req.Profile.DisplayName,
	// 	req.Profile.Description,
	// 	req.Profile.Pronouns,
	// 	req.Profile.Avatar,
	// 	req.Profile.CreatedAt.AsTime(),
	// 	now,
	// 	now,
	// ).WithContext(ctx).Exec(); err != nil {
	// 	logger.Error("failed to create profile", "did", req.Profile.Did, "err", err)
	// 	return &vyletdatabase.CreateProfileResponse{
	// 		Error: helpers.ToStringPtr(err.Error()),
	// 	}, nil
	// }

	return &vyletdatabase.CreatePostResponse{}, nil
}

func (s *Server) DeletePost(ctx context.Context, req *vyletdatabase.DeletePostRequest) (*vyletdatabase.DeletePostResponse, error) {
	// logger := s.logger.With("name", "DeleteProfile")
	//
	// if err := s.cqlSession.Query(
	// 	`
	// 	DELETE FROM profiles
	// 	WHERE
	// 		did = ?
	// 	`,
	// 	req.Did,
	// ).WithContext(ctx).Exec(); err != nil {
	// 	logger.Error("failed to delete profile", "did", req.Did, "err", err)
	// 	return &vyletdatabase.DeleteProfileResponse{
	// 		Error: helpers.ToStringPtr(err.Error()),
	// 	}, nil
	// }

	return &vyletdatabase.DeletePostResponse{}, nil
}

func (s *Server) GetPost(ctx context.Context, req *vyletdatabase.GetPostRequest) (*vyletdatabase.GetPostResponse, error) {
	// logger := s.logger.With("name", "GetProfile")
	//
	// resp := &vyletdatabase.GetProfileResponse{
	// 	Profile: &vyletdatabase.Profile{},
	// }
	// var createdAt, indexedAt time.Time
	//
	// if err := s.cqlSession.Query(
	// 	`SELECT
	// 		did,
	// 		display_name,
	// 		description,
	// 		pronouns,
	// 		avatar,
	// 		created_at,
	// 		indexed_at
	// 	FROM profiles
	// 	WHERE
	// 		did = ?
	// 	`,
	// 	req.Did,
	// ).WithContext(ctx).Scan(
	// 	&resp.Profile.Did,
	// 	&resp.Profile.DisplayName,
	// 	&resp.Profile.Description,
	// 	&resp.Profile.Pronouns,
	// 	&resp.Profile.Avatar,
	// 	&createdAt,
	// 	&indexedAt,
	// ); err != nil {
	// 	logger.Error("failed to get profile", "did", req.Did, "err", err)
	// 	return &vyletdatabase.GetProfileResponse{
	// 		Error: helpers.ToStringPtr(err.Error()),
	// 	}, nil
	// }
	//
	// resp.Profile.CreatedAt = timestamppb.New(createdAt)
	// resp.Profile.IndexedAt = timestamppb.New(indexedAt)

	return nil, nil
}

func (s *Server) GetPosts(ctx context.Context, req *vyletdatabase.GetPostsRequest) (*vyletdatabase.GetPostsResponse, error) {
	return nil, nil
}
