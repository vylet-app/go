package server

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

var (
	ErrDatabaseNotFound  = errors.New("not found")
	ErrInternalServerErr = echo.NewHTTPError(http.StatusInternalServerError, "internal server error")
	ErrInvalidInput      = echo.NewHTTPError(http.StatusBadRequest, "invalid input")
	ErrNotFound          = echo.NewHTTPError(http.StatusNotFound, "not found")
	ErrUnauthorized      = echo.NewHTTPError(http.StatusUnauthorized, "unauthorized")
)

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type ValidationErrors struct {
	Errors []ValidationError `json:"errors"`
}

func (v ValidationErrors) Error() string {
	return "validation failed"
}

func NewValidationError(field, message string) *echo.HTTPError {
	return echo.NewHTTPError(http.StatusBadRequest, ValidationErrors{
		Errors: []ValidationError{{Field: field, Message: message}},
	})
}

func NewValidationErrors(errors ...ValidationError) *echo.HTTPError {
	return echo.NewHTTPError(http.StatusBadRequest, ValidationErrors{
		Errors: errors,
	})
}
