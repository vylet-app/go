package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
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
				Name:     "lexgen-package-url",
				Usage:    "Go package URL for generated lexgen types, used for output schemas",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "lexgen-package-name",
				Usage:    "Name of the package for generated lexgen types, used for output schemas",
				Required: true,
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
		IgnoredLexicons   []string
		LexiconsPath      string
		OutPath           string
		LexgenPackageUrl  string
		LexgenPackageName string
		PackageName       string
	}{
		IgnoredLexicons:   cmd.StringSlice("ignored-lexicons"),
		LexiconsPath:      cmd.String("lexicons-path"),
		OutPath:           cmd.String("out-path"),
		LexgenPackageUrl:  cmd.String("lexgen-package-url"),
		LexgenPackageName: cmd.String("lexgen-package-name"),
		PackageName:       cmd.String("package-name"),
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

	if err := os.Mkdir(args.OutPath, 0744); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("failed to create out directory: %w", err)
		}
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
		filename := strings.ToLower(getName(id)) + ".go"
		filepath := args.OutPath + "/" + filename
		if err := os.WriteFile(filepath, []byte(generateHandler("get", id, def, args.PackageName)), 0644); err != nil {
			return fmt.Errorf("failed to generate file: %w", err)
		}
	}

	for id, def := range procedureDefs {
		filename := strings.ToLower(getName(id)) + ".go"
		filepath := args.OutPath + "/" + filename
		if err := os.WriteFile(filepath, []byte(generateHandler("post", id, def, args.PackageName)), 0644); err != nil {
			return fmt.Errorf("failed to generate file: %w", err)
		}
	}

	filepath := args.OutPath + "/" + args.PackageName + ".go"
	if err := os.WriteFile(filepath, []byte(generateMain(args.PackageName, args.LexgenPackageUrl, args.LexgenPackageName, queryDefs, procedureDefs)), 0644); err != nil {
		return fmt.Errorf("failed to generate file: %w", err)
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

func getTypePartsFromRef(ref string) (string, string) {
	pts := strings.Split(ref, "#")
	nsid := pts[0]
	refName := pts[1]

	return getName(nsid), capitalizeFirst(refName)
}

func generateHandler(method string, id string, def *lex.TypeSchema, packageName string) string {
	name := getName(id)
	structTagPrefix := "query"
	if method == "post" {
		structTagPrefix = "json"
	}

	contents := fmt.Sprintf(`// GENERATED CODE - DO NOT MODIFY
// Generated by vylet-app/handlergen

package %s

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type %sInput struct {
`, packageName, name)
	_ = contents

	sortedParamNames := make([]string, 0, len(def.Parameters.Properties))
	for paramName := range def.Parameters.Properties {
		sortedParamNames = append(sortedParamNames, paramName)
	}
	sort.Slice(sortedParamNames, func(i, j int) bool {
		return sortedParamNames[j] > sortedParamNames[i]
	})

	for _, paramName := range sortedParamNames {
		subDef := def.Parameters.Properties[paramName]
		var typeStr = typeToTypeStr(subDef)
		structTag := structTagPrefix + ":" + "\"" + paramName
		if !slices.Contains(def.Parameters.Required, paramName) {
			typeStr = "*" + typeStr
			if method == "post" {
				structTag += "omitempty"
			}
		}
		structTag += "\""

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

func generateMain(packageName, lexgenPackageUrl, lexgenPackageName string, queryDefs map[string]*lex.TypeSchema, procedureDefs map[string]*lex.TypeSchema) string {
	queryIds := make([]string, 0, len(queryDefs))
	procedureIds := make([]string, 0, len(procedureDefs))
	for id := range queryDefs {
		queryIds = append(queryIds, id)
	}
	for id := range procedureDefs {
		procedureIds = append(procedureIds, id)
	}
	sort.Slice(queryIds, func(i, j int) bool {
		return queryIds[j] > queryIds[i]
	})
	sort.Slice(procedureIds, func(i, j int) bool {
		return procedureIds[j] > procedureIds[i]
	})

	contents := fmt.Sprintf(`// GENERATED CODE - DO NOT MODIFY
// Generated by vylet-app/handlergen

package %s

import (
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"
	%s "%s"
)

type Server interface {
	Logger() *slog.Logger

`, packageName, lexgenPackageName, lexgenPackageUrl)

	addHandlerToInterface := func(id string, refName string) {
		name := getName(id)
		handlerName := "Handle" + name
		inputTypeName := name + "Input"
		requiresAuthName := name + "RequiresAuth"

		var outputTypeName string
		if refName != "" {
			refNsidName, refTypeName := getTypePartsFromRef(refName)
			outputTypeName = lexgenPackageName + "." + refNsidName + "_" + refTypeName
		} else {
			outputTypeName = lexgenPackageName + "." + name + "_Output"
		}

		contents += fmt.Sprintf(`	%s(e echo.Context, input *%s) (*%s, *echo.HTTPError)
	%s() bool
`, handlerName, inputTypeName, outputTypeName, requiresAuthName)
	}

	for _, id := range queryIds {
		schema := queryDefs[id]
		switch schema.Output.Schema.Type {
		case "object":
			addHandlerToInterface(id, "")
		case "ref":
			addHandlerToInterface(id, schema.Output.Schema.Ref)
		}
	}

	for _, id := range procedureIds {
		schema := procedureDefs[id]
		switch schema.Output.Schema.Type {
		case "object":
			addHandlerToInterface(id, "")
		case "ref":
			addHandlerToInterface(id, schema.Output.Schema.Ref)
		}
	}

	contents += `}

type Handlers struct {
	server Server
}

func RegisterHandlers(e *echo.Echo, s Server) {
	h := Handlers{
		server: s,
	}

`
	addHandlerRegistration := func(method string, id string) {
		name := getName(id)
		handlerName := "Handle" + name
		requiresAuthName := name + "RequiresAuth"

		contents += fmt.Sprintf(`	e.%s("/xrpc/%s", h.%s, CreateAuthRequiredMiddleware(s.%s()))
`, method, id, handlerName, requiresAuthName)
	}

	for _, id := range queryIds {
		addHandlerRegistration("GET", id)
	}

	for _, id := range procedureIds {
		addHandlerRegistration("POST", id)
	}

	contents += `}

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
