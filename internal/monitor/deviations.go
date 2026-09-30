package monitor

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// DefaultDeviationThreshold is how long a gate command may remain unmatched by
// observed position before a deviation is raised.
const DefaultDeviationThreshold = 30 * time.Second

const maxResolvedHistory = 100

type DeviationStatus string

const (
	DeviationActive   DeviationStatus = "active"
	DeviationResolved DeviationStatus = "resolved"
)

// GateDeviation is a reviewable record that an observed gate position has not
// matched its commanded target.
type GateDeviation struct {
	ID              uuid.UUID          `json:"id"`
	ChamberID       string             `json:"chamber_id"`
	Direction       model.Direction    `json:"direction"`
	Device          model.DeviceKind   `json:"device"`
	CommandID       uuid.UUID          `json:"command_id"`
	CycleID         uuid.UUID          `json:"cycle_id"`
	Generation      uint64             `json:"generation"`
	Epoch           uint64             `json:"epoch"`
	Action          string             `json:"action"`
	TargetPosition  model.GatePosition `json:"target_position"`
	ActualPosition  model.GatePosition `json:"actual_position"`
	Status          DeviationStatus    `json:"status"`
	Acknowledged    bool               `json:"acknowledged"`
	StartedAt       time.Time          `json:"started_at"`
	DetectedAt      time.Time          `json:"detected_at"`
	LastObservedAt  time.Time          `json:"last_observed_at"`
	AcknowledgedAt  *time.Time         `json:"acknowledged_at,omitempty"`
	ResolvedAt      *time.Time         `json:"resolved_at,omitempty"`
	DurationSeconds int64              `json:"duration_seconds"`
	Reason          string             `json:"reason,omitempty"`
}

// DeviationWatch correlates gate commands with independently observed
// positions and owns the lifecycle of deviation alerts.
type DeviationWatch struct {
	mu        sync.RWMutex
	chambers  []string
	commands  *gate.Commands
	state     *gate.State
	recorder  *journal.Recorder
	now       func() time.Time
	threshold time.Duration
	active    map[string]*GateDeviation
	resolved  []GateDeviation
}

func NewDeviationWatch(chambers []string, commands *gate.Commands, state *gate.State, recorder *journal.Recorder, threshold time.Duration, now func() time.Time) *DeviationWatch {
	if threshold <= 0 {
		threshold = DefaultDeviationThreshold
	}
	if now == nil {
		now = time.Now
	}
	return &DeviationWatch{
		chambers:  append([]string(nil), chambers...),
		commands:  commands,
		state:     state,
		recorder:  recorder,
		now:       now,
		threshold: threshold,
		active:    make(map[string]*GateDeviation),
	}
}

var gateDevices = []model.DeviceKind{model.DeviceUpstreamGate, model.DeviceDownstreamGate}

func deviceDirection(device model.DeviceKind) model.Direction {
	if device == model.DeviceDownstreamGate {
		return model.DirectionDownstream
	}
	return model.DirectionUpstream
}

func deviationKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

// OnGateChange implements gate.ChangeSink so every command/observation change
// triggers an immediate re-evaluation.
func (w *DeviationWatch) OnGateChange(chamberID string, _ model.Direction) {
	w.Evaluate(chamberID)
}

type deviationEvent struct {
	deviation GateDeviation
	kind      model.EventType
}

// Evaluate reconciles open deviations against the latest pending gate command
// and observed position for one chamber. Repeated unmatched observations only
// refresh the existing alert; reaching the target (or command termination)
// resolves it.
func (w *DeviationWatch) Evaluate(chamberID string) []GateDeviation {
	now := w.now().UTC()

	// Read command/position state outside the watch lock.
	latest := map[model.DeviceKind]model.DeviceCommand{}
	for _, command := range w.commands.Pending(chamberID) {
		if command.Device != model.DeviceUpstreamGate && command.Device != model.DeviceDownstreamGate {
			continue
		}
		current, exists := latest[command.Device]
		if !exists || command.Epoch > current.Epoch {
			latest[command.Device] = command
		}
	}
	observed := make(map[model.DeviceKind]model.GatePosition, len(gateDevices))
	observedAt := make(map[model.DeviceKind]time.Time, len(gateDevices))
	for _, device := range gateDevices {
		record := w.state.Position(chamberID, deviceDirection(device))
		observed[device] = record.Position
		observedAt[device] = record.UpdatedAt
	}

	var events []deviationEvent
	var activeResult []GateDeviation

	w.mu.Lock()
	for _, device := range gateDevices {
		key := deviationKey(chamberID, device)
		open := w.active[key]
		command, hasCommand := latest[device]

		// The alert is bound to a command. Settlement, cancellation or a newer
		// command superseding it closes the open alert.
		if open != nil && (!hasCommand || open.CommandID != command.ID) {
			reason := "command cancelled"
			if hasCommand {
				reason = "superseded"
			}
			events = append(events, deviationEvent{
				deviation: w.resolveLocked(open, now, reason),
				kind:      model.EventGateDeviationResolved,
			})
			open = nil
		}

		if !hasCommand {
			continue
		}

		target := model.GateClosed
		if command.Action == "open" {
			target = model.GateOpen
		}
		actual := observed[device]

		if actual == target {
			if open != nil {
				events = append(events, deviationEvent{
					deviation: w.resolveLocked(open, now, "matched"),
					kind:      model.EventGateDeviationResolved,
				})
			}
			continue
		}

		if open != nil {
			// Same unmatched command: refresh observations only, never raise
			// a duplicate alert.
			open.ActualPosition = actual
			open.LastObservedAt = observedAt[device]
			activeResult = append(activeResult, *open)
			continue
		}

		if now.Sub(command.CreatedAt.UTC()) < w.threshold {
			continue
		}

		alert := &GateDeviation{
			ID:             uuid.New(),
			ChamberID:      chamberID,
			Direction:      deviceDirection(device),
			Device:         device,
			CommandID:      command.ID,
			CycleID:        command.CycleID,
			Generation:     command.Generation,
			Epoch:          command.Epoch,
			Action:         command.Action,
			TargetPosition: target,
			ActualPosition: actual,
			Status:         DeviationActive,
			StartedAt:      command.CreatedAt.UTC(),
			DetectedAt:     now,
			LastObservedAt: observedAt[device],
		}
		w.active[key] = alert
		activeResult = append(activeResult, *alert)
		events = append(events, deviationEvent{deviation: *alert, kind: model.EventGateDeviationRaised})
	}
	w.mu.Unlock()

	for _, event := range events {
		_, _ = w.recorder.Record(context.Background(), event.deviation.ChamberID, event.kind,
			event.deviation.CycleID, event.deviation.Generation, event.deviation)
	}
	return activeResult
}

func (w *DeviationWatch) resolveLocked(open *GateDeviation, now time.Time, reason string) GateDeviation {
	open.Status = DeviationResolved
	open.ResolvedAt = &now
	open.Reason = reason
	resolved := *open
	delete(w.active, deviationKey(open.ChamberID, open.Device))
	w.resolved = append(w.resolved, resolved)
	if len(w.resolved) > maxResolvedHistory {
		w.resolved = w.resolved[len(w.resolved)-maxResolvedHistory:]
	}
	return resolved
}

// List re-evaluates every known chamber (so elapsed thresholds and cancelled
// commands are picked up without a background timer) and returns active alerts
// followed by recently resolved ones.
func (w *DeviationWatch) List() []GateDeviation {
	for _, chamberID := range w.chambers {
		w.Evaluate(chamberID)
	}
	w.mu.RLock()
	result := make([]GateDeviation, 0, len(w.active)+len(w.resolved))
	for _, open := range w.active {
		result = append(result, *open)
	}
	result = append(result, w.resolved...)
	now := w.now().UTC()
	w.mu.RUnlock()

	for i := range result {
		result[i].DurationSeconds = durationSeconds(result[i], now)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Status != result[j].Status {
			return result[i].Status == DeviationActive
		}
		left, right := result[i].DetectedAt, result[j].DetectedAt
		return left.After(right)
	})
	return result
}

func durationSeconds(deviation GateDeviation, now time.Time) int64 {
	end := now
	if deviation.ResolvedAt != nil {
		end = *deviation.ResolvedAt
	}
	return int64(end.Sub(deviation.StartedAt).Seconds())
}

// Acknowledge marks an active deviation as operator-handled. It does not close
// the alert; that still happens automatically once the position matches.
func (w *DeviationWatch) Acknowledge(chamberID string, commandID uuid.UUID) (GateDeviation, error) {
	now := w.now().UTC()
	w.mu.Lock()
	var acknowledged *GateDeviation
	for _, open := range w.active {
		if open.ChamberID == chamberID && open.CommandID == commandID {
			acknowledged = open
			break
		}
	}
	if acknowledged == nil {
		w.mu.Unlock()
		return GateDeviation{}, errors.New("no active deviation for command")
	}
	acknowledged.Acknowledged = true
	acknowledged.AcknowledgedAt = &now
	updated := *acknowledged
	w.mu.Unlock()

	_, _ = w.recorder.Record(context.Background(), updated.ChamberID, model.EventGateDeviationAcknowledged,
		updated.CycleID, updated.Generation, updated)
	updated.DurationSeconds = durationSeconds(updated, now)
	return updated, nil
}
