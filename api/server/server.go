package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	slogecho "github.com/samber/slog-echo"
	"github.com/vylet-app/go/database/client"
	"github.com/vylet-app/go/generated/handlers"
	"golang.org/x/time/rate"
)

type Server struct {
	logger    *slog.Logger
	httpd     *http.Server
	echo      *echo.Echo
	client    *client.Client
	directory *identity.CacheDirectory
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

	initSigningMethods()

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

	baseDirectory := identity.BaseDirectory{
		PLCURL: "https://plc.directory",
		HTTPClient: http.Client{
			Timeout: time.Second * 5,
		},
		PLCLimiter:            rate.NewLimiter(rate.Limit(10), 1),
		TryAuthoritativeDNS:   false,
		SkipDNSDomainSuffixes: []string{".bsky.social", ".staging.bsky.dev"},
	}
	directory := identity.NewCacheDirectory(&baseDirectory, 100_000, time.Hour*48, time.Minute*15, time.Minute*15)

	server := Server{
		logger:    logger,
		echo:      echo,
		httpd:     &httpd,
		client:    client,
		directory: &directory,
	}

	server.echo.HTTPErrorHandler = server.errorHandler
	server.echo.Use(server.didAuthMiddleware())

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

func (s *Server) Logger() *slog.Logger {
	return s.logger
}

func (s *Server) registerHandlers() {
	handlers.RegisterHandlers(s.echo, s)

	// app.vylet.media
	s.echo.GET("/xrpc/app.vylet.media.getBlob/:did/:cid", s.handleGetBlob)
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

type AtProtoClaims struct {
	Sub string `json:"sub"`
	Aud string `json:"aud"`
	Iss string `json:"iss"`
	jwt.RegisteredClaims
}

func (s *Server) getKeyForDid(ctx context.Context, did syntax.DID) (atcrypto.PublicKey, error) {
	ident, err := s.directory.LookupDID(ctx, did)
	if err != nil {
		return nil, err
	}
	return ident.PublicKey()
}

func (s *Server) fetchKey(ctx context.Context) func(tok *jwt.Token) (any, error) {
	return func(tok *jwt.Token) (any, error) {
		issuer, ok := tok.Claims.(jwt.MapClaims)["iss"].(string)
		if !ok {
			return nil, fmt.Errorf("missing 'iss' field from auth header JWT")
		}

		did, err := syntax.ParseDID(issuer)
		if err != nil {
			return nil, fmt.Errorf("invalid DID in 'iss' field from auth header JWT")
		}

		k, err := s.getKeyForDid(ctx, did)
		if err != nil {
			return nil, fmt.Errorf("failed to look up public key for DID (%q): %w", did, err)
		}

		return k, nil
	}
}

func (s *Server) checkJwt(ctx context.Context, token string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	validMethods := []string{"ES256K", "ES256"}
	config := []jwt.ParserOption{jwt.WithValidMethods(validMethods)}

	p := jwt.NewParser(config...)
	t, err := p.Parse(token, s.fetchKey(ctx))
	if err != nil {
		return "", fmt.Errorf("failed to parse auth header jwt: %w", err)
	}

	clms, ok := t.Claims.(jwt.MapClaims)
	if !ok {
		return "", fmt.Errorf("invalid token claims")
	}

	did, ok := clms["iss"].(string)
	if !ok {
		return "", fmt.Errorf("no issuer present in returned claims")
	}

	return did, nil
}

func (s *Server) didAuthMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(e echo.Context) error {
			authHeader := e.Request().Header.Get("Authorization")

			if authHeader == "" {
				return next(e)
			}

			if !strings.HasPrefix(authHeader, "Bearer ") {
				return echo.NewHTTPError(http.StatusUnauthorized, "Invalid authorization format")
			}

			tokenString := strings.TrimPrefix(authHeader, "Bearer ")

			ctx := e.Request().Context()
			userDid, err := s.checkJwt(ctx, tokenString)
			if err != nil {
				return echo.NewHTTPError(http.StatusUnauthorized,
					fmt.Sprintf("Token verification failed: %v", err))
			}

			e.Set("viewer", userDid)

			return next(e)
		}
	}
}

func getViewer(e echo.Context) string {
	viewer, ok := e.Get("viewer").(string)
	if ok {
		return viewer
	}
	return ""
}
