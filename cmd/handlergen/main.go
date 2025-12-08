package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bluesky-social/indigo/lex"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "handlergen",
		Usage: "a small CLI tool to generate Echo framework handlers from input lexicon",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "ignored-lexicons",
				Usage: "lexicon to ignore while generating handlers. may be a full NSID or wildcard, i.e. com.atproto.*",
			},
			&cli.StringFlag{
				Name:      "lexicons-path",
				Usage:     "path to the lexicons that you wish to generate handlers for",
				Aliases:   []string{"lexicons", "lp", "l"},
				TakesFile: true,
				Required:  true,
			},
			&cli.StringFlag{
				Name:    "out-path",
				Usage:   "path to which the handlers will be written",
				Aliases: []string{"out", "o"},
				Value:   "handlers",
			},
			&cli.StringFlag{
				Name:  "package-name",
				Usage: "if specified, a package name to use. if none is provided, the out directory name will be the package name",
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(cmd *cli.Context) error {
	args := struct {
		IgnoredLexicons []string
		LexiconsPath    string
		OutPath         string
		PackageName     string
	}{
		IgnoredLexicons: cmd.StringSlice("ignored-lexicons"),
		LexiconsPath:    cmd.String("lexicons-path"),
		OutPath:         cmd.String("out-path"),
		PackageName:     cmd.String("package-name"),
	}

	if args.PackageName == "" {
		args.PackageName = args.OutPath
	}

	ignoredLexicons := make([]string, 0, len(args.IgnoredLexicons))
	for _, lex := range args.IgnoredLexicons {
		if strings.HasSuffix(lex, ".*") {
			ignoredLexicons = append(ignoredLexicons, strings.TrimSuffix(lex, ".*"))
		} else {
			ignoredLexicons = append(ignoredLexicons, lex)
		}
	}

	foundSchemaFiles, err := findSchemas(args.LexiconsPath, []string{})
	if err != nil {
		return fmt.Errorf("failed to find schemas: %w", err)
	}

	var schemas []*lex.Schema
	for _, schemaFile := range foundSchemaFiles {
		s, err := lex.ReadSchema(schemaFile)
		if err != nil {
			return fmt.Errorf("failed to read schemas: %w", err)
		}
		schemas = append(schemas, s)
	}

	queryDefs := make(map[string]*lex.TypeSchema)
	procedureDefs := make(map[string]*lex.TypeSchema)
	for _, s := range schemas {
		skip := false
		for _, lex := range ignoredLexicons {
			if s.ID == lex || strings.HasPrefix(s.ID, lex) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		for _, def := range s.Defs {
			switch def.Type {
			case "query":
				queryDefs[s.ID] = def
			case "procedure":
				procedureDefs[s.ID] = def
			default:
				continue
			}
		}
	}

	for id, def := range queryDefs {
		generateHandler("get", id, def, args.PackageName)
	}

	return nil
}

func findSchemas(dir string, out []string) ([]string, error) {
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".json") {
			out = append(out, path)
		}

		return nil
	})
	if err != nil {
		return out, err
	}

	return out, nil
}

func capitalizeFirst(str string) string {
	return strings.ToUpper(str[:1]) + str[1:]
}

func getName(id string) string {
	pts := strings.Split(id, ".")
	last := pts[len(pts)-1]
	second := pts[len(pts)-2]

	last = strings.ToUpper(last[:1]) + last[1:]
	second = strings.ToUpper(second[:1]) + second[1:]

	return second + last
}

func generateHandler(method string, id string, def *lex.TypeSchema, packageName string) string {
	name := getName(id)
	structTagPrefix := "query"
	if method == "post" {
		structTagPrefix = "json"
	}
	_ = name
	// fileName := strings.ToLower("handle" + name + ".go")
	// handlerName := "Handle" + name

	contents := fmt.Sprintf(`package %s

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type %sInput struct {
`, packageName, name)
	_ = contents

	for paramName, subDef := range def.Parameters.Properties {
		var typeStr = typeToTypeStr(subDef)

		structTag := structTagPrefix + ":" + paramName

		if !slices.Contains(def.Parameters.Required, paramName) {
			typeStr = "*" + typeStr
			if method == "post" {
				structTag += "omitempty"
			}
		}

		contents += fmt.Sprintf("\t%s %s `%s`\n", capitalizeFirst(paramName), typeStr, structTag)
	}

	contents += fmt.Sprintf(`}

func (h *%s) Handle%s(e echo.Context) error {
	var input %sInput
	if err := e.Bind(&input); err != nil {
		logger := h.server.Logger().With("handler", "Handle%s")
		logger.Error("error binding request", "err", err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Internal Server Error")
	}

	output, err := h.server.Handle%s(e, &input)
	if err != nil {
		return err
	}

	return e.JSON(http.StatusOK, &output)
}
`, capitalizeFirst(packageName), name, name, name, name)

	fmt.Println(contents, "\n\n")

	return contents
}

func typeToTypeStr(def *lex.TypeSchema) string {
	switch def.Type {
	case "string":
		return "string"
	case "integer":
		return "int64"
	case "boolean":
		return "bool"
	case "array":
		return "[]" + typeToTypeStr(def.Items)
	}
	return ""
}

func generateMain() string {
	contents := `package handlers

import (
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/vylet-app/go/generated/vylet"
)

type Server interface {
	Logger() *slog.Logger

`

	contents += `
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
`
	contents += `
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
`
	return contents
}
