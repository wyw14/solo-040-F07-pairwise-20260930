package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func acknowledgeDeviationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		commandID, err := parseUUID(chi.URLParam(r, "commandID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		deviation, err := runtime.Deviations.Acknowledge(chamberID, commandID)
		if err != nil {
			writeError(w, http.StatusConflict, errors.New("active deviation not found"))
			return
		}
		writeJSON(w, http.StatusOK, deviation)
	}
}
