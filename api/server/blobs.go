package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
)

type MediaGetBlobInput struct {
	Did string `param:"did"`
	Cid string `param:"cid"`
}

func (s *Server) handleGetBlob(e echo.Context) error {
	ctx := e.Request().Context()
	logger := s.logger.With("name", "handleGetBlob")

	var input MediaGetBlobInput
	if err := e.Bind(&input); err != nil {
		return ErrInternalServerErr
	}

	if input.Did == "" {
		return NewValidationError("did", "did parameter is required")
	}

	if input.Cid == "" {
		return NewValidationError("cid", "cid parameter is required")
	}

	// Validate DID format
	parsedDid, err := syntax.ParseDID(input.Did)
	if err != nil {
		return NewValidationError("did", "invalid DID format")
	}
	did := parsedDid.String()

	// Validate CID format
	parsedCid, err := syntax.ParseCID(input.Cid)
	if err != nil {
		return NewValidationError("cid", "invalid CID format")
	}
	cid := parsedCid.String()

	// Fetch blob metadata from database
	resp, err := s.client.BlobRef.GetBlobRef(ctx, &vyletdatabase.GetBlobRefRequest{
		Did: did,
		Cid: cid,
	})
	if err != nil {
		logger.Error("error getting blob ref from database", "did", did, "cid", cid, "err", err)
		return ErrInternalServerErr
	}

	if resp.Error != nil {
		if client.IsNotFoundError(resp.Error) {
			return ErrNotFound
		}
		logger.Error("error getting blob ref", "did", did, "cid", cid, "error", *resp.Error)
		return ErrInternalServerErr
	}

	// Check if blob is taken down
	if resp.BlobRef.TakenDown {
		return echo.NewHTTPError(http.StatusGone, "blob has been taken down")
	}

	// Resolve PDS endpoint from DID
	pdsEndpoint, err := s.getPdsEndpoint(ctx, did)
	if err != nil {
		logger.Error("error resolving PDS endpoint", "did", did, "err", err)
		return ErrInternalServerErr
	}

	// Construct blob URL and redirect
	blobUrl := fmt.Sprintf("%s/xrpc/com.atproto.sync.getBlob?did=%s&cid=%s", pdsEndpoint, did, cid)
	return e.Redirect(http.StatusFound, blobUrl)
}

func (s *Server) getPdsEndpoint(ctx context.Context, did string) (string, error) {
	parsed, err := syntax.ParseDID(did)
	if err != nil {
		return "", fmt.Errorf("failed to parse DID: %w", err)
	}

	doc, err := s.directory.LookupDID(ctx, parsed)
	if err != nil {
		return "", fmt.Errorf("failed to fetch DID document: %w", err)
	}

	serviceEndpoint := doc.GetServiceEndpoint("atproto_pds")
	if serviceEndpoint == "" {
		return "", fmt.Errorf("no PDS endpoing found in DID document")
	}

	return serviceEndpoint, nil
}
