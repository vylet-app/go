package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/go-util/pkg/robusthttp"
	"github.com/haileyok/cocoon/identity"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	slogecho "github.com/samber/slog-echo"
	"github.com/vylet-app/go/database/client"
)

type Server struct {
	logger   *slog.Logger
	httpd    *http.Server
	echo     *echo.Echo
	client   *client.Client
	passport *identity.Passport
}

type Args struct {
	Logger *slog.Logger
	Addr   string
	DbHost string
}

func New(args *Args) (*Server, error) {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	logger := args.Logger

	echo := echo.New()
	echo.Use(middleware.RemoveTrailingSlash())
	echo.Use(middleware.Recover())
	echo.Use(echoprometheus.NewMiddleware(""))
	echo.Use(slogecho.New(logger))

	httpd := http.Server{
		Addr:    args.Addr,
		Handler: echo,
	}

	client, err := client.New(&client.Args{
		Addr: args.DbHost,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create new database client: %w", err)
	}

	passport := identity.NewPassport(robusthttp.NewClient(), identity.NewMemCache(10_000))

	server := Server{
		logger:   logger,
		echo:     echo,
		httpd:    &httpd,
		client:   client,
		passport: passport,
	}

	server.echo.HTTPErrorHandler = server.errorHandler

	server.registerHandlers()

	return &server, nil
}

func (s *Server) Run(ctx context.Context) error {
	logger := s.logger.With("name", "Run")

	shutdownEcho := make(chan struct{}, 1)
	echoShutdown := make(chan struct{}, 1)
	go func() {
		logger := s.logger.With("component", "echo")

		logger.Info("starting api server", "addr", s.httpd.Addr)

		go func() {
			if err := s.httpd.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("error listening", "err", err)
				close(shutdownEcho)
			}
		}()

		<-shutdownEcho

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := s.echo.Shutdown(shutdownCtx); err != nil {
			logger.Error("error shutting down echo", "err", err)
		}

		close(echoShutdown)
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGABRT)

	select {
	case sig := <-signals:
		logger.Info("received exit signal", "signal", sig)
	case <-ctx.Done():
		logger.Info("context cancelled")
	case <-shutdownEcho:
		logger.Warn("server shut down unexpectedly")
	}

	logger.Info("server shut down successfully")

	return nil
}

func (s *Server) registerHandlers() {
	// app.vylet.actor
	s.echo.GET("/xrpc/app.vylet.actor.getProfile", s.handleGetProfile)
	s.echo.GET("/xrpc/app.vylet.actor.getProfiles", s.handleGetProfiles)
}

func (s *Server) errorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}

	code := http.StatusInternalServerError
	var message any = map[string]string{"error": "internal server error"}

	if he, ok := err.(*echo.HTTPError); ok {
		code = he.Code

		if validationErrs, ok := he.Message.(ValidationErrors); ok {
			message = validationErrs
		} else {
			message = map[string]any{
				"error": he.Message,
			}
		}
	} else {
		c.Logger().Error(err)
	}

	if err := c.JSON(code, message); err != nil {
		c.Logger().Error(err)
	}
}
