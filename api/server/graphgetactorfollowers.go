package server

import (
	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/generated/handlers"
	"github.com/vylet-app/go/generated/vylet"
)

func (s *Server) GraphGetActorFollowersRequiresAuth() bool {
	return false
}

func (s *Server) HandleGraphGetActorFollowers(e echo.Context, input *handlers.GraphGetActorFollowersInput) (*vylet.GraphGetActorFollowers_Output, *echo.HTTPError) {
	ctx := e.Request().Context()

	logger := s.logger.With("name", "HandleGraphGetActorFollowers")

	return &vylet.GraphGetActorFollowers_Output{}, nil
}
