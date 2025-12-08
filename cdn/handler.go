package cdn

import (
	"context"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	vyletkafka "github.com/vylet-app/go/bus/proto"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) handleEvent(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	if evt.Commit != nil {
		return s.handleCommit(ctx, evt)
	}

	return nil
}

func (s *Server) handleCommit(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	logger := s.logger.With("did", evt.Did, "collection", evt.Commit.Collection, "rkey", evt.Commit.Rkey)

	op := evt.Commit
	switch op.Operation {
	case vyletkafka.CommitOperation_COMMIT_OPERATION_CREATE, vyletkafka.CommitOperation_COMMIT_OPERATION_UPDATE:
		// Track record processing
		recordsProcessed.WithLabelValues(op.Operation.String()).Inc()

		rec, err := atdata.UnmarshalJSON(op.Record)
		if err != nil {
			logger.Error("failed to unmarshal record JSON", "err", err)
			return nil // Don't fail the event processing, just skip blob extraction
		}

		// Extract blobs from the record
		blobs := atdata.ExtractBlobs(rec)

		if len(blobs) == 0 {
			// No blobs in this record, nothing to do
			return nil
		}

		// Track blobs extracted
		blobsExtracted.Add(float64(len(blobs)))
		logger.Debug("extracted blobs from record", "count", len(blobs))

		// Store each blob reference in the database
		now := time.Now().UTC()
		for _, blob := range blobs {
			cid := blob.Ref.String()

			// Check if blob ref already exists
			getResp, err := s.db.BlobRef.GetBlobRef(ctx, &vyletdatabase.GetBlobRefRequest{
				Did: evt.Did,
				Cid: cid,
			})
			if err != nil {
				logger.Error("failed to check if blob ref exists", "cid", cid, "err", err)
				continue
			}

			// If blob ref doesn't exist, create it
			if getResp.Error != nil {
				createResp, err := s.db.BlobRef.CreateBlobRef(ctx, &vyletdatabase.CreateBlobRefRequest{
					BlobRef: &vyletdatabase.BlobRef{
						Did:         evt.Did,
						Cid:         cid,
						FirstSeenAt: timestamppb.New(now),
						TakenDown:   false,
					},
				})
				if err != nil {
					dbOperations.WithLabelValues("create", "error").Inc()
					logger.Error("failed to create blob ref", "cid", cid, "err", err)
					continue
				}
				if createResp.Error != nil {
					dbOperations.WithLabelValues("create", "error").Inc()
					logger.Error("error creating blob ref", "cid", cid, "error", *createResp.Error)
					continue
				}

				dbOperations.WithLabelValues("create", "success").Inc()
				logger.Debug("created blob ref", "cid", cid)
			} else {
				// Blob ref already exists, update the updated_at timestamp
				updateResp, err := s.db.BlobRef.UpdateBlobRef(ctx, &vyletdatabase.UpdateBlobRefRequest{
					BlobRef: &vyletdatabase.BlobRef{
						Did:       evt.Did,
						Cid:       cid,
						TakenDown: getResp.BlobRef.TakenDown,
					},
				})
				if err != nil {
					dbOperations.WithLabelValues("update", "error").Inc()
					logger.Error("failed to update blob ref", "cid", cid, "err", err)
					continue
				}
				if updateResp.Error != nil {
					dbOperations.WithLabelValues("update", "error").Inc()
					logger.Error("error updating blob ref", "cid", cid, "error", *updateResp.Error)
					continue
				}

				dbOperations.WithLabelValues("update", "success").Inc()
				logger.Debug("updated blob ref", "cid", cid)
			}
		}

	case vyletkafka.CommitOperation_COMMIT_OPERATION_DELETE:
		// For deletes, we don't remove blob refs since they might be referenced elsewhere
		// We could potentially track reference counts in the future
		return nil
	}

	return nil
}
