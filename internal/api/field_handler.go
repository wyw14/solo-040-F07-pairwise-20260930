package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

func closeGateHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		direction := model.Direction(chi.URLParam(r, "direction"))
		if !direction.Valid() {
			writeError(w, http.StatusBadRequest, errors.New("invalid gate direction"))
			return
		}
		cycleValue, ok := runtime.CycleService.Current(chamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		command, err := runtime.Interlocks.RequestClose(r.Context(), cycleValue, direction)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, command)
	}
}

type positionInput struct {
	Position model.GatePosition `json:"position"`
}

func gateObservationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input positionInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		direction := model.Direction(chi.URLParam(r, "direction"))
		if !direction.Valid() || !validPosition(input.Position) {
			writeError(w, http.StatusBadRequest, errors.New("invalid direction or position"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		if err := runtime.Interlocks.Observe(chamberID, direction, input.Position, runtime.Now()); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		if err := runtime.Deviations.Assess(r.Context(), chamberID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, input)
	}
}

func validPosition(position model.GatePosition) bool {
	return position == model.GateOpen || position == model.GateOpening ||
		position == model.GateClosed || position == model.GateClosing
}

func releaseReservationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		reservation, ok := runtime.Interlocks.Reservation(chamberID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("active reservation not found"))
			return
		}
		if err := runtime.Interlocks.Release(chamberID, reservation.Owner); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, reservation)
	}
}

type pendingInput struct {
	Count int `json:"count"`
}

func equipmentPendingHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input pendingInput
		if err := decodeJSON(r, &input); err != nil || input.Count <= 0 || input.Count > 128 {
			writeError(w, http.StatusBadRequest, errors.New("count must be between 1 and 128"))
			return
		}
		chamberID := chi.URLParam(r, "chamberID")
		runtime.Safety.Barrier().Add(chamberID, input.Count)
		writeJSON(w, http.StatusAccepted, map[string]int{"pending": runtime.Safety.Barrier().Pending(chamberID)})
	}
}

func equipmentSettledHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		runtime.Safety.Barrier().Settled(chamberID)
		writeJSON(w, http.StatusOK, map[string]int{"pending": runtime.Safety.Barrier().Pending(chamberID)})
	}
}

type handoffInput struct {
	CycleID    string `json:"cycle_id"`
	Generation uint64 `json:"generation"`
}

func signalHandoffHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input handoffInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		cycleID, err := parseUUID(input.CycleID)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		handoff := signal.Handoff{
			ChamberID: chi.URLParam(r, "chamberID"), CycleID: cycleID,
			Generation: input.Generation, ObservedAt: time.Now().UTC(),
		}
		if err := runtime.Signals.ConfirmHandoff(r.Context(), handoff); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, handoff)
	}
}
