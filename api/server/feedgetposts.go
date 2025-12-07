package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/database/client"
	vyletdatabase "github.com/vylet-app/go/database/proto"
	"github.com/vylet-app/go/generated/vylet"
	"github.com/vylet-app/go/internal/helpers"
)

type GetFeedPostsInput struct {
	Uris []string `query:"uris"`
}

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
	logger := s.logger.With("name", "feedPostsToPostViews")

	resp, err := s.client.Post.GetPosts(ctx, &vyletdatabase.GetPostsRequest{
		Uris: uris,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get post: %w", err)
	}
	if client.IsNotFoundError(resp.Error) {
		return nil, ErrDatabaseNotFound
	}

	feedPostViews := make(map[string]*vylet.FeedDefs_PostView)
	var wg sync.WaitGroup
	var lk sync.Mutex
	for _, post := range resp.Posts {
		wg.Go(func() {
			aturi, _ := syntax.ParseATURI(post.Uri)
			profileBasic, err := s.getProfileBasic(ctx, aturi.Authority().String())
			if err != nil {
				logger.Error("failed to get profile basic", "did", aturi.Authority().String(), "err", err)
				return
			}

			postView := &vylet.FeedDefs_PostView{
				Author:  profileBasic,
				Caption: post.Caption,
				Cid:     post.Cid,
				Facets:  []*vylet.RichtextFacet{},
				// Labels:     []*atproto.LabelDefs_Label{},
				Media:      &vylet.FeedDefs_PostView_Media{},
				LikeCount:  new(int64),
				ReplyCount: new(int64),
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

			if post.Facets != nil {
				var facets []*vylet.RichtextFacet
				if err := json.Unmarshal(post.Facets, &facets); err != nil {
					logger.Error("failed to unmarshal post facets", "uri", post.Uri, "err", err)
					return
				}
				postView.Facets = facets
			}

			lk.Lock()
			defer lk.Unlock()
			feedPostViews[post.Uri] = postView
		})
	}

	return feedPostViews, nil
}

func (s *Server) handleGetPosts(e echo.Context) error {
	ctx := e.Request().Context()

	logger := s.logger.With("name", "handleGetPost")

	var input GetFeedPostsInput
	if err := e.Bind(&input); err != nil {
		logger.Error("failed to bind", "err", err)
		return ErrInternalServerErr
	}

	if len(input.Uris) == 0 {
		return NewValidationError("uris", "must supply at least one AT-URI")
	}

	if len(input.Uris) > 25 {
		return NewValidationError("uris", "no more than 25 AT-URIs may be supplied")
	}

	if allValid, err := helpers.ValidateUris(input.Uris); !allValid {
		logger.Warn("received invalid URIs", "uris", input.Uris, "err", err)
		return NewValidationError("uris", "all URIs must be valid AT-URIs")
	}

	postViews, err := s.getPostViews(ctx, input.Uris, "")
	if err != nil {
		logger.Error("failed to get posts", "err", err)
		return ErrInternalServerErr
	}

	if len(postViews) == 0 {
		return ErrNotFound
	}

	return e.JSON(200, postViews)
}
