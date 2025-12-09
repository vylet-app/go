package server

import (
	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/generated/handlers"
	"github.com/vylet-app/go/generated/vylet"
)

func (s *Server) GraphGetActorFollowsRequiresAuth() bool {
	return false
}

func (s *Server) HandleGraphGetActorFollows(e echo.Context, input *handlers.GraphGetActorFollowsInput) (*vylet.GraphGetActorFollows_Output, *echo.HTTPError) {
	ctx := e.Request().Context()

	logger := s.logger.With("name", "HandleGraphGetActorFollows")

	return &vylet.GraphGetActorFollows_Output{}, nil
}
