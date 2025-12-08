package handlers

import (
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/generated/vylet"
)

type Server interface {
	Logger() *slog.Logger

	HandleFeedGetActorPosts(e echo.Context, input *FeedGetActorPostsInput) (*vylet.FeedGetActorPosts_Output, *echo.HTTPError)
	FeedGetActorPostsRequiresAuth() bool
}

type Handlers struct {
	server Server
}

func RegisterHandlers(e *echo.Echo, s Server) {
	h := Handlers{
		server: s,
	}

	e.GET("/xrpc/app.vylet.feed.getActorPosts", h.HandleFeedGetActorPosts, CreateAuthRequiredMiddleware(s.FeedGetActorPostsRequiresAuth()))
}

func AuthRequiredMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(e echo.Context) error {
		viewer, ok := e.Get("viewer").(string)
		if !ok || viewer == "" {
			return echo.NewHTTPError(http.StatusUnauthorized, "Unauthorized")
		}
		return next(e)
	}
}

func CreateAuthRequiredMiddleware(authRequired bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		if authRequired {
			return AuthRequiredMiddleware(next)
		} else {
			return func(e echo.Context) error {
				return next(e)
			}
		}
	}
}
