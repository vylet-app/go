package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/bluesky-social/indigo/lex/util"
	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
	"github.com/vylet-app/go/handlers"
	"github.com/vylet-app/go/internal/helpers"
	"golang.org/x/sync/errgroup"
)

func (s *Server) getPosts(ctx context.Context, uris []string) (map[string]*vylet.FeedPost, error) {
	resp, err := s.client.Post.GetPosts(ctx, &vyletdatabase.GetPostsRequest{
		Uris: uris,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get post: %w", err)
	}
	if client.IsNotFoundError(resp.Error) {
		return nil, ErrDatabaseNotFound
	}

	feedPosts := make(map[string]*vylet.FeedPost)
	for _, post := range resp.Posts {
		feedPost := &vylet.FeedPost{
			Caption:   post.Caption,
			CreatedAt: post.CreatedAt.AsTime().Format(time.RFC3339Nano),
			Media:     &vylet.FeedPost_Media{},
		}

		if post.Images == nil {
			return nil, fmt.Errorf("bad post, contains no media")
		}

		media := vylet.FeedPost_Media{
			MediaImages: &vylet.MediaImages{
				Images: make([]*vylet.MediaImages_Image, 0, len(post.Images)),
			},
		}

		for _, img := range post.Images {
			mediaImg := &vylet.MediaImages_Image{
				Image: &util.LexBlob{
					Ref:      helpers.StrToLexLink(img.Cid),
					MimeType: img.Mime,
					Size:     img.Size,
				},
			}
			if img.Alt != nil {
				mediaImg.Alt = *img.Alt
			}
			if img.Width != nil && img.Height != nil {
				mediaImg.AspectRatio = &vylet.MediaDefs_AspectRatio{
					Width:  *img.Width,
					Height: *img.Height,
				}
			}

			media.MediaImages.Images = append(media.MediaImages.Images, mediaImg)
		}

		if post.Facets != nil {
			var facets []*vylet.RichtextFacet
			if err := json.Unmarshal(post.Facets, &facets); err != nil {
				return nil, fmt.Errorf("failed to unmarshal post facets: %w", err)
			}
			feedPost.Facets = facets
		}
	}

	return feedPosts, nil
}

func (s *Server) getPostViews(ctx context.Context, uris []string, viewer string) (map[string]*vylet.FeedDefs_PostView, error) {
	resp, err := s.client.Post.GetPosts(ctx, &vyletdatabase.GetPostsRequest{
		Uris: uris,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get post: %w", err)
	}
	if client.IsNotFoundError(resp.Error) {
		return nil, ErrDatabaseNotFound
	}

	feedPostViews, err := s.postsToPostViews(ctx, resp.Posts, viewer)
	if err != nil {
		return nil, err
	}

	return feedPostViews, nil
}

func (s *Server) postsToPostViews(ctx context.Context, posts map[string]*vyletdatabase.Post, viewer string) (map[string]*vylet.FeedDefs_PostView, error) {
	logger := s.logger.With("name", "postsToPostViews")

	uris := make([]string, 0, len(posts))
	dids := make([]string, 0, len(posts))
	addedDids := make(map[string]struct{})
	for uri, post := range posts {
		uris = append(uris, uri)

		if _, ok := addedDids[post.AuthorDid]; ok {
			continue
		}
		dids = append(dids, post.AuthorDid)
		addedDids[post.AuthorDid] = struct{}{}
	}

	g, gCtx := errgroup.WithContext(ctx)
	var profiles map[string]*vylet.ActorDefs_ProfileViewBasic
	var countsResp *vyletdatabase.GetPostsInteractionCountsResponse
	g.Go(func() error {
		maybeProfiles, err := s.getProfilesBasic(gCtx, dids)
		if err != nil {
			return err
		}
		profiles = maybeProfiles
		return nil
	})
	g.Go(func() error {
		maybeCounts, err := s.client.Post.GetPostsInteractionCounts(gCtx, &vyletdatabase.GetPostsInteractionCountsRequest{Uris: uris})
		if err != nil {
			return err
		}
		if maybeCounts.Error != nil {
			return fmt.Errorf("failed to get post interaction counts: %s", *maybeCounts.Error)
		}
		countsResp = maybeCounts
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("error getting metadata: %w", err)
	}

	feedPostViews := make(map[string]*vylet.FeedDefs_PostView)
	for _, post := range posts {
		profileBasic, ok := profiles[post.AuthorDid]
		if !ok {
			logger.Warn("failed to get profile for post", "did", post.AuthorDid, "uri", post.Uri)
			continue
		}
		counts, ok := countsResp.Counts[post.Uri]
		if !ok {
			logger.Warn("failed to get counts for post", "uri", post.Uri)
			continue
		}

		postView := &vylet.FeedDefs_PostView{
			Author:  profileBasic,
			Caption: post.Caption,
			Cid:     post.Cid,
			Facets:  []*vylet.RichtextFacet{},
			// Labels:     []*atproto.LabelDefs_Label{},
			Media:      &vylet.FeedDefs_PostView_Media{},
			LikeCount:  counts.Likes,
			ReplyCount: counts.Replies,
			Uri:        post.Uri,
			// Viewer:     &vylet.FeedDefs_ViewerState{
			// 	Like: new(string),
			// },
			CreatedAt: post.CreatedAt.AsTime().Format(time.RFC3339Nano),
			IndexedAt: post.IndexedAt.AsTime().Format(time.RFC3339Nano),
		}

		media := vylet.FeedDefs_PostView_Media{
			MediaImages_View: &vylet.MediaImages_View{
				Images: make([]*vylet.MediaImages_ViewImage, 0, len(post.Images)),
			},
		}
		for _, img := range post.Images {
			mediaImg := &vylet.MediaImages_ViewImage{
				Alt:       img.Alt,
				Fullsize:  helpers.ImageCidToCdnUrl(img.Cid, "fullsize"),
				Thumbnail: helpers.ImageCidToCdnUrl(img.Cid, "thumb"),
			}
			if img.Width != nil && img.Height != nil {
				mediaImg.AspectRatio = &vylet.MediaDefs_AspectRatio{
					Width:  *img.Width,
					Height: *img.Height,
				}
			}

			media.MediaImages_View.Images = append(media.MediaImages_View.Images, mediaImg)
		}
		postView.Media = &media

		if post.Facets != nil {
			var facets []*vylet.RichtextFacet
			if err := json.Unmarshal(post.Facets, &facets); err != nil {
				logger.Error("failed to unmarshal post facets", "uri", post.Uri, "err", err)
				continue
			}
			postView.Facets = facets
		}

		feedPostViews[post.Uri] = postView
	}

	return feedPostViews, nil
}

func (s *Server) FeedGetPostsRequiresAuth() bool {
	return false
}

func (s *Server) HandleFeedGetPosts(e echo.Context, input *handlers.FeedGetPostsInput) (*vylet.FeedGetPosts_Output, *echo.HTTPError) {
	ctx := e.Request().Context()
	viewer := getViewer(e)

	logger := s.logger.With("name", "HandleFeedGetPosts", "viewer", viewer)

	if len(input.Uris) == 0 {
		return nil, NewValidationError("uris", "must supply at least one AT-URI")
	}

	if len(input.Uris) > 25 {
		return nil, NewValidationError("uris", "no more than 25 AT-URIs may be supplied")
	}

	if allValid, err := helpers.ValidateUris(input.Uris); !allValid {
		logger.Warn("received invalid URIs", "uris", input.Uris, "err", err)
		return nil, NewValidationError("uris", "all URIs must be valid AT-URIs")
	}

	postViews, err := s.getPostViews(ctx, input.Uris, viewer)
	if err != nil {
		logger.Error("failed to get posts", "err", err)
		return nil, ErrInternalServerErr
	}

	if len(postViews) == 0 {
		return nil, ErrNotFound
	}

	orderedPostViews := make([]*vylet.FeedDefs_PostView, 0, len(postViews))
	for _, uri := range input.Uris {
		postView, ok := postViews[uri]
		if !ok {
			logger.Warn("failed to find post for uri", "uri", uri)
			continue
		}
		orderedPostViews = append(orderedPostViews, postView)
	}

	return &vylet.FeedGetPosts_Output{
		Posts: orderedPostViews,
	}, nil
}

func (s *Server) FeedGetActorPostsRequiresAuth() bool {
	return false
}

func (s *Server) HandleFeedGetActorPosts(e echo.Context, input *handlers.FeedGetActorPostsInput) (*vylet.FeedGetActorPosts_Output, *echo.HTTPError) {
	ctx := e.Request().Context()
	viewer := getViewer(e)

	logger := s.logger.With("name", "handleGetActorPosts", "viewer", viewer)

	if input.Limit != nil && (*input.Limit < 1 || *input.Limit > 100) {
		return nil, NewValidationError("limit", "limit must be between 1 and 100")
	} else if input.Limit == nil {
		input.Limit = helpers.ToInt64Ptr(25)
	}

	logger = logger.With("actor", input.Actor, "limit", *input.Limit, "cursor", input.Cursor)

	did, _, err := s.fetchDidHandleFromActor(ctx, input.Actor)
	if err != nil {
		if errors.Is(err, ErrActorNotValid) {
			return nil, NewValidationError("actor", "actor must be a valid DID or handle")
		}
		logger.Error("error fetching did and handle", "err", err)
		return nil, ErrInternalServerErr
	}

	resp, err := s.client.Post.GetPostsByActor(ctx, &vyletdatabase.GetPostsByActorRequest{
		Did:    did,
		Limit:  *input.Limit,
		Cursor: input.Cursor,
	})
	if err != nil {
		logger.Error("failed to get posts", "did", did)
		return nil, ErrInternalServerErr
	}

	postViews, err := s.postsToPostViews(ctx, resp.Posts, viewer)
	if err != nil {
		s.logger.Error("failed to get post views", "err", err)
		return nil, ErrInternalServerErr
	}

	sortedPostViews := make([]*vylet.FeedDefs_PostView, 0, len(postViews))
	for _, postView := range postViews {
		sortedPostViews = append(sortedPostViews, postView)
	}
	sort.Slice(sortedPostViews, func(i, j int) bool {
		return sortedPostViews[i].CreatedAt > sortedPostViews[j].CreatedAt
	})

	return &vylet.FeedGetActorPosts_Output{
		Posts:  sortedPostViews,
		Cursor: resp.Cursor,
	}, nil
}
