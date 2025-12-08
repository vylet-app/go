package server

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/internal/helpers"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) GetBlobRef(ctx context.Context, req *vyletdatabase.GetBlobRefRequest) (*vyletdatabase.GetBlobRefResponse, error) {
	logger := s.logger.With("name", "GetBlobRef", "did", req.Did, "cid", req.Cid)

	query := `
		SELECT did, cid, first_seen_at, processed_at, updated_at, taken_down, takedown_reason, taken_down_at, tags
		FROM blob_refs
		WHERE did = ? AND cid = ?
	`

	blobRef := &vyletdatabase.BlobRef{}
	var firstSeenAt, updatedAt time.Time
	var processedAt, takenDownAt *time.Time
	var tags []string

	err := s.cqlSession.Query(query, req.Did, req.Cid).WithContext(ctx).Scan(
		&blobRef.Did,
		&blobRef.Cid,
		&firstSeenAt,
		&processedAt,
		&updatedAt,
		&blobRef.TakenDown,
		&blobRef.TakedownReason,
		&takenDownAt,
		&tags,
	)

	if err != nil {
		if err == gocql.ErrNotFound {
			logger.Warn("blob ref not found", "did", req.Did, "cid", req.Cid)
			return &vyletdatabase.GetBlobRefResponse{
				Error: helpers.ToStringPtr("blob ref not found"),
			}, nil
		}
		logger.Error("failed to fetch blob ref", "did", req.Did, "cid", req.Cid, "err", err)
		return &vyletdatabase.GetBlobRefResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	blobRef.FirstSeenAt = timestamppb.New(firstSeenAt)
	blobRef.UpdatedAt = timestamppb.New(updatedAt)
	if processedAt != nil {
		blobRef.ProcessedAt = timestamppb.New(*processedAt)
	}
	if takenDownAt != nil {
		blobRef.TakenDownAt = timestamppb.New(*takenDownAt)
	}
	blobRef.Tags = tags

	return &vyletdatabase.GetBlobRefResponse{
		BlobRef: blobRef,
	}, nil
}

func (s *Server) CreateBlobRef(ctx context.Context, req *vyletdatabase.CreateBlobRefRequest) (*vyletdatabase.CreateBlobRefResponse, error) {
	logger := s.logger.With("name", "CreateBlobRef", "did", req.BlobRef.Did, "cid", req.BlobRef.Cid)

	now := time.Now().UTC()

	var processedAt, takenDownAt *time.Time
	if req.BlobRef.ProcessedAt != nil {
		t := req.BlobRef.ProcessedAt.AsTime()
		processedAt = &t
	}
	if req.BlobRef.TakenDownAt != nil {
		t := req.BlobRef.TakenDownAt.AsTime()
		takenDownAt = &t
	}

	query := `
		INSERT INTO blob_refs
			(did, cid, first_seen_at, processed_at, updated_at, taken_down, takedown_reason, taken_down_at, tags)
		VALUES
			(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	err := s.cqlSession.Query(query,
		req.BlobRef.Did,
		req.BlobRef.Cid,
		req.BlobRef.FirstSeenAt.AsTime(),
		processedAt,
		now,
		req.BlobRef.TakenDown,
		req.BlobRef.TakedownReason,
		takenDownAt,
		req.BlobRef.Tags,
	).WithContext(ctx).Exec()

	if err != nil {
		logger.Error("failed to create blob ref", "did", req.BlobRef.Did, "cid", req.BlobRef.Cid, "err", err)
		return &vyletdatabase.CreateBlobRefResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.CreateBlobRefResponse{}, nil
}

func (s *Server) UpdateBlobRef(ctx context.Context, req *vyletdatabase.UpdateBlobRefRequest) (*vyletdatabase.UpdateBlobRefResponse, error) {
	logger := s.logger.With("name", "UpdateBlobRef", "did", req.BlobRef.Did, "cid", req.BlobRef.Cid)

	now := time.Now().UTC()

	var processedAt, takenDownAt *time.Time
	if req.BlobRef.ProcessedAt != nil {
		t := req.BlobRef.ProcessedAt.AsTime()
		processedAt = &t
	}
	if req.BlobRef.TakenDownAt != nil {
		t := req.BlobRef.TakenDownAt.AsTime()
		takenDownAt = &t
	}

	query := `
		UPDATE blob_refs
		SET processed_at = ?, updated_at = ?, taken_down = ?, takedown_reason = ?, taken_down_at = ?, tags = ?
		WHERE did = ? AND cid = ?
	`

	err := s.cqlSession.Query(query,
		processedAt,
		now,
		req.BlobRef.TakenDown,
		req.BlobRef.TakedownReason,
		takenDownAt,
		req.BlobRef.Tags,
		req.BlobRef.Did,
		req.BlobRef.Cid,
	).WithContext(ctx).Exec()

	if err != nil {
		logger.Error("failed to update blob ref", "did", req.BlobRef.Did, "cid", req.BlobRef.Cid, "err", err)
		return &vyletdatabase.UpdateBlobRefResponse{
			Error: helpers.ToStringPtr(err.Error()),
		}, nil
	}

	return &vyletdatabase.UpdateBlobRefResponse{}, nil
}
