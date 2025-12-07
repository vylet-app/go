package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

var (
	ErrAtHandleNotPresent = errors.New("could not find at handle inside of did doc")
	ErrActorNotValid      = errors.New("actor was not a valid did or handle")
)

func (s *Server) handleFromDid(ctx context.Context, did string) (string, error) {
	doc, err := s.passport.FetchDoc(ctx, did)
	if err != nil {
		return "", fmt.Errorf("failed to fetch did doc: %w", err)
	}
	for _, aka := range doc.AlsoKnownAs {
		if after, ok := strings.CutPrefix(aka, "at://"); ok {
			return after, nil
		}
	}
	return "", ErrAtHandleNotPresent
}

// Given either a valid DID or handle, finds both the DID and handle for said actor and returns them.
// Returns ErrActorNotValid if the actor is not a valid DID or handle.
func (s *Server) fetchDidHandleFromActor(ctx context.Context, actor string) (string, string, error) {
	logger := s.logger.With("name", "fetchDidHandleForActor", "actor", actor)

	var did string
	var handle string
	if parsed, err := syntax.ParseDID(actor); err == nil {
		did = parsed.String()
	} else if parsed, err := syntax.ParseHandle(actor); err == nil {
		handle = parsed.String()
	}

	logger = logger.With("did", did, "handle", handle)

	if did == "" && handle == "" {
		logger.Error("actor was not a valid did or handle")
		return "", "", ErrActorNotValid
	}

	if did != "" {
		maybeHandle, err := s.handleFromDid(ctx, did)
		if err != nil {
			logger.Error("error getting handle", "err", err)
			return "", "", err
		}
		handle = maybeHandle
	} else if handle != "" {
		maybeDid, err := s.passport.ResolveHandle(ctx, handle)
		if err != nil {
			logger.Error("error getting did", "err", err)
			return "", "", err
		}
		did = maybeDid
	}

	return did, handle, nil
}
