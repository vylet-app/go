package indexer

import (
	"context"

	vyletkafka "github.com/vylet-app/go/bus/proto"
)

func (s *Server) handleEvent(ctx context.Context, evt *vyletkafka.FirehoseEvent) error {
	if evt.Commit != nil {
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
	case "app.vylet.graph.follow":
		return s.handleGraphFollow(ctx, evt)
	}

	return nil
}
