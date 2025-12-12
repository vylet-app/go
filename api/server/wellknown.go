package server

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Server) handleDidJson(e echo.Context) error {
	return e.JSON(http.StatusOK, map[string]any{
		"@context": []string{
			"https://www.w3.org/ns/did/v1",
			"https://w3id.org/security/multikey/v1",
		},
		"id": "did:web:staging.vylet.app",
		"service": []map[string]string{
			{
				"id":              "#vylet_appview",
				"type":            "VyletAppView",
				"serviceEndpoint": "https://staging.vylet.app",
			},
		},
	})
}
