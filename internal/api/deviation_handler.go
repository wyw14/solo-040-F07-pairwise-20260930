package api

import (
	"net/http"

	"github.com/wyw14/cry-96/internal/model"
)

// deviationView adds the running episode duration to the stored alarm.
type deviationView struct {
	model.DeviationAlarm
	DurationSeconds float64 `json:"duration_seconds"`
}

func deviationsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := runtime.Now().UTC()
		result := make(map[string][]deviationView, len(runtime.Chambers))
		for _, chamberID := range runtime.Chambers {
			// Pull-based assessment catches deviations that crossed the
			// threshold while no new observations arrived.
			if err := runtime.Deviations.Assess(r.Context(), chamberID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			alarms := runtime.Deviations.List(chamberID)
			views := make([]deviationView, 0, len(alarms))
			for _, alarm := range alarms {
				views = append(views, deviationView{alarm, alarm.Duration(now).Seconds()})
			}
			result[chamberID] = views
		}
		writeJSON(w, http.StatusOK, result)
	}
}
