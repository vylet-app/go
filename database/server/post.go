package server

import (
	"context"

	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
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
	logger := s.logger.With("name", "DeletePost")

	if err := s.cqlSession.Query(
		`
		DELETE FROM posts 
		WHERE
			uri = ?
		`,
		req.Uri,
	).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to delete post", "uri", req.Uri, "err", err)
		return &vyletdatabase.DeletePostResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.DeletePostResponse{}, nil
}

func (s *Server) GetPosts(ctx context.Context, req *vyletdatabase.GetPostsRequest) (*vyletdatabase.GetPostsResponse, error) {
	return nil, nil
}
