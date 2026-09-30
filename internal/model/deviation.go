package model

import (
	"time"

	"github.com/google/uuid"
)

type DeviationStatus string

const (
	DeviationActive   DeviationStatus = "active"
	DeviationResolved DeviationStatus = "resolved"
)

// DeviationAlarm records one continuous episode in which device feedback
// disagreed with a gate command's target position. Repeated observations
// refresh the same alarm; a match or a terminal command resolves it.
type DeviationAlarm struct {
	ID          uuid.UUID       `json:"id"`
	ChamberID   string          `json:"chamber_id"`
	CommandID   uuid.UUID       `json:"command_id"`
	CycleID     uuid.UUID       `json:"cycle_id"`
	Generation  uint64          `json:"generation"`
	Device      DeviceKind      `json:"device"`
	Action      string          `json:"action"`
	Target      GatePosition    `json:"target"`
	Observed    GatePosition    `json:"observed"`
	Status      DeviationStatus `json:"status"`
	FirstSeenAt time.Time       `json:"first_seen_at"`
	RaisedAt    time.Time       `json:"raised_at"`
	LastSeenAt  time.Time       `json:"last_seen_at"`
	ResolvedAt  *time.Time      `json:"resolved_at,omitempty"`
	Resolution  string          `json:"resolution,omitempty"`
}

// Duration reports how long the deviation episode lasted. Active alarms
// measure against now; resolved alarms stop at their resolution time.
func (a DeviationAlarm) Duration(now time.Time) time.Duration {
	end := now
	if a.ResolvedAt != nil {
		end = *a.ResolvedAt
	}
	if end.Before(a.FirstSeenAt) {
		return 0
	}
	return end.Sub(a.FirstSeenAt)
}
