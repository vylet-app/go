package indexer

import (
	"context"

	vyletkafka "github.com/vylet-app/go/bus/proto"
)

func (s *Server) handleEvent(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	logger := s.logger.With("name", "handleEvent")
	if evt.Commit != nil {
		err := s.handleCommit(ctx, evt)
		if err != nil {
			logger.Error("error handling event", "err", err)
			return err
		}
		return s.handleCommit(ctx, evt)
	}

	return nil
}

func (s *Server) handleCommit(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	switch evt.Commit.Collection {
	case "app.vylet.actor.profile":
		return s.handleActorProfile(ctx, evt)
	case "app.vylet.feed.post":
		return s.handleFeedPost(ctx, evt)
	case "app.vylet.feed.like":
		return s.handleFeedLike(ctx, evt)
	}

	return nil
}
