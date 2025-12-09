package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gocql/gocql"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) CreateLike(ctx context.Context, req *vyletdatabase.CreateLikeRequest) (*vyletdatabase.CreateLikeResponse, error) {
	logger := s.logger.With("name", "CreateLike")

	aturi, err := syntax.ParseATURI(req.Like.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse aturi: %w", err)
	}

	did := aturi.Authority().String()
	now := time.Now().UTC()

	batch := s.cqlSession.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	likeArgs := []any{
		req.Like.Uri,
		req.Like.Cid,
		req.Like.SubjectUri,
		req.Like.SubjectCid,
		did,
		req.Like.CreatedAt.AsTime(),
		now,
	}

	likeQuery := `
		INSERT INTO %s
			(uri, cid, subject_uri, subject_cid, author_did, created_at, indexed_at)
		VALUES
			(?, ?, ?, ?, ?, ?, ?)
	`

	batch.Query(fmt.Sprintf(likeQuery, "likes_by_subject"), likeArgs...)
	batch.Query(fmt.Sprintf(likeQuery, "likes_by_actor"), likeArgs...)
	batch.Query(fmt.Sprintf(likeQuery, "likes_by_uri"), likeArgs...)
	batch.Query(fmt.Sprintf(likeQuery, "likes_by_actor_subject"), likeArgs...)

	if err := s.cqlSession.ExecuteBatch(batch); err != nil {
		logger.Error("failed to create like", "uri", req.Like.Uri, "err", err)
		return &vyletdatabase.CreateLikeResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	if err := s.cqlSession.Query(`
		UPDATE post_interaction_counts
		SET like_count = like_count + 1
		WHERE post_uri = ?
	`, req.Like.SubjectUri).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to increment like count", "subject_uri", req.Like.SubjectUri, "err", err)
		return &vyletdatabase.CreateLikeResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreateLikeResponse{}, nil
}

func (s *Server) DeleteLike(ctx context.Context, req *vyletdatabase.DeleteLikeRequest) (*vyletdatabase.DeleteLikeResponse, error) {
	logger := s.logger.With("name", "DeleteLike", "uri", req.Uri)

	var (
		createdAt  time.Time
		subjectUri string
		authorDid  string
	)

	query := `
		SELECT created_at, subject_uri, author_did
		FROM likes_by_uri
		WHERE uri = ?
	`
	if err := s.cqlSession.Query(query, req.Uri).WithContext(ctx).Scan(&createdAt, &subjectUri, &authorDid); err != nil {
		if err == gocql.ErrNotFound {
			logger.Warn("like not found", "uri", req.Uri)
			return &vyletdatabase.DeleteLikeResponse{
				Error: helpers.ToStringPtr("like not found"),
			}, nil
		}
		logger.Error("failed to fetch like", "uri", req.Uri, "err", err)
		return &vyletdatabase.DeleteLikeResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	batch := s.cqlSession.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	batch.Query(`
		DELETE FROM likes_by_uri
		WHERE uri = ?
	`, req.Uri)

	batch.Query(`
		DELETE FROM likes_by_subject
		WHERE subject_uri = ? AND created_at = ? AND uri = ?
	`, subjectUri, createdAt, req.Uri)

	batch.Query(`
		DELETE FROM likes_by_actor
		WHERE author_did = ? AND created_at = ? AND uri = ?
	`, authorDid, createdAt, req.Uri)

	batch.Query(`
		DELETE FROM likes_by_actor_subject
		WHERE author_did = ? AND subject_uri = ?
		`, authorDid, subjectUri)

	if err := s.cqlSession.ExecuteBatch(batch); err != nil {
		logger.Error("failed to delete like", "uri", req.Uri, "err", err)
		return &vyletdatabase.DeleteLikeResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	if err := s.cqlSession.Query(`
		UPDATE post_interaction_counts
		SET like_count = like_count - 1
		WHERE post_uri = ?
	`, subjectUri).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to increment like count", "subject_uri", subjectUri, "err", err)
		return &vyletdatabase.DeleteLikeResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.DeleteLikeResponse{}, nil
}

func (s *Server) GetLikesBySubject(ctx context.Context, req *vyletdatabase.GetLikesBySubjectRequest) (*vyletdatabase.GetLikesBySubjectResponse, error) {
	logger := s.logger.With("name", "GetLikesBySubject", "subjectUri", req.SubjectUri)

	if req.Limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}

	var (
		query string
		args  []any
	)

	if req.Cursor != nil && *req.Cursor != "" {
		cursorParts := strings.SplitN(*req.Cursor, "|", 2)
		if len(cursorParts) != 2 {
			logger.Error("invalid cursor format", "cursor", *req.Cursor)
			return &vyletdatabase.GetLikesBySubjectResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}

		cursorTime, err := time.Parse(time.RFC3339Nano, cursorParts[0])
		if err != nil {
			logger.Error("failed to parse cursor timestamp", "cursor", *req.Cursor, "err", err)
			return &vyletdatabase.GetLikesBySubjectResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}
		cursorUri := cursorParts[1]

		query = `
			SELECT uri, cid, subject_uri, subject_cid, author_did, created_at, indexed_at
			FROM likes_by_subject
			WHERE subject_uri = ? AND (created_at, uri) < (?, ?)
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.SubjectUri, cursorTime, cursorUri, req.Limit + 1}
	} else {
		query = `
			SELECT uri, cid, subject_uri, subject_cid, author_did, created_at, indexed_at
			FROM likes_by_subject
			WHERE subject_uri = ?
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.SubjectUri, req.Limit}
	}

	iter := s.cqlSession.Query(query, args...).WithContext(ctx).Iter()
	defer iter.Close()

	var likes []*vyletdatabase.Like

	var createdAt time.Time
	var indexedAt time.Time
	for {
		like := &vyletdatabase.Like{}
		if !iter.Scan(
			&like.Uri,
			&like.Cid,
			&like.SubjectUri,
			&like.SubjectCid,
			&like.AuthorDid,
			&createdAt,
			&indexedAt,
		) {
			break
		}
		like.CreatedAt = timestamppb.New(createdAt)
		like.IndexedAt = timestamppb.New(indexedAt)

		likes = append(likes, like)
	}
	if err := iter.Close(); err != nil {
		logger.Error("failed to iterate likes", "err", err)
		return &vyletdatabase.GetLikesBySubjectResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	var nextCursor *string
	if len(likes) > int(req.Limit) {
		likes = likes[:req.Limit]
		lastLike := likes[len(likes)-1]
		cursorStr := fmt.Sprintf("%s|%s",
			lastLike.CreatedAt.AsTime().Format(time.RFC3339Nano),
			lastLike.Uri)
		nextCursor = &cursorStr
	}

	return &vyletdatabase.GetLikesBySubjectResponse{
		Likes:  likes,
		Cursor: nextCursor,
	}, nil
}
