package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) CreateFollow(ctx context.Context, req *vyletdatabase.CreateFollowRequest) (*vyletdatabase.CreateFollowResponse, error) {
	logger := s.logger.With("name", "CreateFollow", "uri", req.Follow.Uri, "did", req.Follow.AuthorDid, "subjectDid", req.Follow.SubjectDid)

	now := time.Now().UTC()

	batch := s.cqlSession.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	likeArgs := []any{
		req.Follow.Uri,
		req.Follow.Cid,
		req.Follow.SubjectDid,
		req.Follow.AuthorDid,
		req.Follow.CreatedAt.AsTime(),
		now,
	}

	likeQuery := `
		INSERT INTO %s
			(uri, cid, subject_did, author_did, created_at, indexed_at)
		VALUES
			(?, ?, ?, ?, ?, ?)
	`

	batch.Query(fmt.Sprintf(likeQuery, "follows_by_subject_did"), likeArgs...)
	batch.Query(fmt.Sprintf(likeQuery, "follows_by_author_did"), likeArgs...)
	batch.Query(fmt.Sprintf(likeQuery, "follows_by_uri"), likeArgs...)

	if err := s.cqlSession.ExecuteBatch(batch); err != nil {
		logger.Error("failed to create follow", "err", err)
		return &vyletdatabase.CreateFollowResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	if err := s.cqlSession.Query(`
		UPDATE follow_counts
		SET follows_count = follows_count + 1
		WHERE did = ?
	`, req.Follow.AuthorDid).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to increment follows count", "err", err)
		return &vyletdatabase.CreateFollowResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	if err := s.cqlSession.Query(`
		UPDATE follow_counts
		SET followers_count = followers_count + 1
		WHERE did = ?
	`, req.Follow.SubjectDid).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to increment followers count", "err", err)
		return &vyletdatabase.CreateFollowResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreateFollowResponse{}, nil
}

func (s *Server) DeleteFollow(ctx context.Context, req *vyletdatabase.DeleteFollowRequest) (*vyletdatabase.DeleteFollowResponse, error) {
	logger := s.logger.With("name", "DeleteFollow", "uri", req.Uri)

	var (
		createdAt  time.Time
		subjectDid string
		authorDid  string
	)

	query := `
		SELECT created_at, subject_did, author_did
		FROM follow_by_uri
		WHERE uri = ?
	`
	if err := s.cqlSession.Query(query, req.Uri).WithContext(ctx).Scan(&createdAt, &subjectDid, &authorDid); err != nil {
		if err == gocql.ErrNotFound {
			logger.Warn("follow not found", "uri", req.Uri)
			return &vyletdatabase.DeleteFollowResponse{
				Error: helpers.ToStringPtr("follow not found"),
			}, nil
		}
		logger.Error("failed to fetch follow", "uri", req.Uri, "err", err)
		return &vyletdatabase.DeleteFollowResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	logger = logger.With("authorDid", authorDid, "subjectDid", subjectDid)

	batch := s.cqlSession.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	batch.Query(`
		DELETE FROM follows_by_uri
		WHERE uri = ?
	`, req.Uri)

	batch.Query(`
		DELETE FROM follows_by_subject_did
		WHERE subject_did = ? AND created_at = ? AND uri = ?
	`, subjectDid, createdAt, req.Uri)

	batch.Query(`
		DELETE FROM likes_by_author_did
		WHERE author_did = ? AND created_at = ? AND uri = ?
	`, authorDid, createdAt, req.Uri)

	if err := s.cqlSession.ExecuteBatch(batch); err != nil {
		logger.Error("failed to follow like", "uri", req.Uri, "err", err)
		return &vyletdatabase.DeleteFollowResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	if err := s.cqlSession.Query(`
		UPDATE follow_counts
		SET follows_count = follows_count - 1
		WHERE did = ?
	`, authorDid).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to increment follows count", "err", err)
		return &vyletdatabase.DeleteFollowResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	if err := s.cqlSession.Query(`
		UPDATE follow_counts
		SET followers_count = followers_count - 1
		WHERE did = ?
	`, subjectDid).WithContext(ctx).Exec(); err != nil {
		logger.Error("failed to increment followers count", "err", err)
		return &vyletdatabase.DeleteFollowResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.DeleteFollowResponse{}, nil
}

func (s *Server) GetFollowsByActor(ctx context.Context, req *vyletdatabase.GetFollowsByActorRequest) (*vyletdatabase.GetFollowsByActorResponse, error) {
	logger := s.logger.With("name", "GetFollowsByActor", "did", req.Did)

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
			return &vyletdatabase.GetFollowsByActorResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}

		cursorTime, err := time.Parse(time.RFC3339Nano, cursorParts[0])
		if err != nil {
			logger.Error("failed to parse cursor timestamp", "cursor", *req.Cursor, "err", err)
			return &vyletdatabase.GetFollowsByActorResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}
		cursorUri := cursorParts[1]

		query = `
			SELECT uri, cid, subject_did, author_did, created_at, indexed_at
			FROM follows_by_author_did
			WHERE author_did = ? AND (created_at, uri) < (?, ?)
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.Did, cursorTime, cursorUri, req.Limit + 1}
	} else {
		query = `
			SELECT uri, cid, subject_did, author_did, created_at, indexed_at
			FROM follows_by_author_did
			WHERE author_did = ?
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.Did, req.Limit}
	}

	iter := s.cqlSession.Query(query, args...).WithContext(ctx).Iter()
	defer iter.Close()

	var follows []*vyletdatabase.Follow

	var (
		createdAt time.Time
		indexedAt time.Time
	)
	for {
		follow := &vyletdatabase.Follow{}
		if !iter.Scan(
			&follow.Uri,
			&follow.Cid,
			&follow.SubjectDid,
			&follow.AuthorDid,
			&createdAt,
			&indexedAt,
		) {
			break
		}
		follow.CreatedAt = timestamppb.New(createdAt)
		follow.IndexedAt = timestamppb.New(indexedAt)

		follows = append(follows, follow)
	}
	if err := iter.Close(); err != nil {
		logger.Error("failed to iterate likes", "err", err)
		return &vyletdatabase.GetFollowsByActorResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	var nextCursor *string
	if len(follows) > int(req.Limit) {
		follows = follows[:req.Limit]
		lastLike := follows[len(follows)-1]
		cursorStr := fmt.Sprintf("%s|%s",
			lastLike.CreatedAt.AsTime().Format(time.RFC3339Nano),
			lastLike.Uri)
		nextCursor = &cursorStr
	}

	return &vyletdatabase.GetFollowsByActorResponse{
		Follows: follows,
		Cursor:  nextCursor,
	}, nil
}

func (s *Server) GetFollowersByActor(ctx context.Context, req *vyletdatabase.GetFollowersByActorRequest) (*vyletdatabase.GetFollowersByActorResponse, error) {
	logger := s.logger.With("name", "GetFollowersByActor", "did", req.Did)

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
			return &vyletdatabase.GetFollowersByActorResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}

		cursorTime, err := time.Parse(time.RFC3339Nano, cursorParts[0])
		if err != nil {
			logger.Error("failed to parse cursor timestamp", "cursor", *req.Cursor, "err", err)
			return &vyletdatabase.GetFollowersByActorResponse{
				Error: helpers.ToStringPtr("invalid cursor format"),
			}, nil
		}
		cursorUri := cursorParts[1]

		query = `
			SELECT uri, cid, subject_did, author_did, created_at, indexed_at
			FROM follows_by_subject_did
			WHERE subject_did = ? AND (created_at, uri) < (?, ?)
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.Did, cursorTime, cursorUri, req.Limit + 1}
	} else {
		query = `
			SELECT uri, cid, subject_did, author_did, created_at, indexed_at
			FROM follows_by_subject_did
			WHERE subject_did = ?
			ORDER BY created_at DESC, uri ASC
			LIMIT ?
		`
		args = []any{req.Did, req.Limit}
	}

	iter := s.cqlSession.Query(query, args...).WithContext(ctx).Iter()
	defer iter.Close()

	var follows []*vyletdatabase.Follow

	var (
		createdAt time.Time
		indexedAt time.Time
	)
	for {
		follow := &vyletdatabase.Follow{}
		if !iter.Scan(
			&follow.Uri,
			&follow.Cid,
			&follow.SubjectDid,
			&follow.AuthorDid,
			&createdAt,
			&indexedAt,
		) {
			break
		}
		follow.CreatedAt = timestamppb.New(createdAt)
		follow.IndexedAt = timestamppb.New(indexedAt)

		follows = append(follows, follow)
	}
	if err := iter.Close(); err != nil {
		logger.Error("failed to iterate likes", "err", err)
		return &vyletdatabase.GetFollowersByActorResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	var nextCursor *string
	if len(follows) > int(req.Limit) {
		follows = follows[:req.Limit]
		lastLike := follows[len(follows)-1]
		cursorStr := fmt.Sprintf("%s|%s",
			lastLike.CreatedAt.AsTime().Format(time.RFC3339Nano),
			lastLike.Uri)
		nextCursor = &cursorStr
	}

	return &vyletdatabase.GetFollowersByActorResponse{
		Follows: follows,
		Cursor:  nextCursor,
	}, nil
}
