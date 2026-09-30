// Package deviation watches pending gate commands against observed gate
// positions and raises reviewable alarms when device feedback stops
// matching the command target for longer than a threshold.
package deviation

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// DefaultThreshold bounds how long feedback may disagree with a command
// target before an alarm is raised.
const DefaultThreshold = 30 * time.Second

// commandTrack remembers the last assessment outcome for one command so a
// match followed by a mismatch starts a fresh deviation episode.
type commandTrack struct {
	matched       bool
	mismatchSince time.Time
}

// Tracker keeps one active alarm per command and resolves it automatically
// once observed feedback confirms the target or the command goes terminal.
type Tracker struct {
	mu        sync.Mutex
	threshold time.Duration
	state     *gate.State
	commands  *gate.Commands
	recorder  *journal.Recorder
	now       func() time.Time
	tracks    map[uuid.UUID]commandTrack
	active    map[uuid.UUID]*model.DeviationAlarm
	resolved  []model.DeviationAlarm
}

func NewTracker(state *gate.State, commands *gate.Commands, recorder *journal.Recorder, threshold time.Duration, now func() time.Time) *Tracker {
	if threshold <= 0 {
		threshold = DefaultThreshold
	}
	if now == nil {
		now = time.Now
	}
	return &Tracker{
		threshold: threshold, state: state, commands: commands, recorder: recorder, now: now,
		tracks: make(map[uuid.UUID]commandTrack), active: make(map[uuid.UUID]*model.DeviationAlarm),
	}
}

// Assess compares every pending gate command of the chamber with the latest
// observed position, raising, refreshing or resolving alarms as needed.
func (t *Tracker) Assess(ctx context.Context, chamberID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := t.now().UTC()
	positions := t.state.Snapshot(chamberID)
	pending := t.commands.Pending(chamberID)

	var raised, resolved []model.DeviationAlarm
	t.mu.Lock()
	pendingIDs := make(map[uuid.UUID]bool, len(pending))
	for _, command := range pending {
		pendingIDs[command.ID] = true
		direction, ok := command.Device.Direction()
		if !ok {
			continue
		}
		target, ok := command.TargetPosition()
		if !ok {
			continue
		}
		observed := positions[direction]
		track, seen := t.tracks[command.ID]
		if observed == target {
			t.tracks[command.ID] = commandTrack{matched: true}
			if alarm, active := t.active[command.ID]; active {
				resolved = append(resolved, t.resolveLocked(alarm, now, "position matched"))
			}
			continue
		}
		switch {
		case !seen:
			// Never assessed before: the target has been awaited since issuance.
			track = commandTrack{mismatchSince: command.CreatedAt}
		case track.matched:
			// Feedback slipped back away from the target: new episode.
			track = commandTrack{mismatchSince: now}
		}
		track.matched = false
		t.tracks[command.ID] = track
		if now.Sub(track.mismatchSince) < t.threshold {
			continue
		}
		if alarm, active := t.active[command.ID]; active {
			alarm.Observed = observed
			alarm.LastSeenAt = now
			continue
		}
		alarm := &model.DeviationAlarm{
			ID: uuid.New(), ChamberID: chamberID, CommandID: command.ID,
			CycleID: command.CycleID, Generation: command.Generation,
			Device: command.Device, Action: command.Action, Target: target,
			Observed: observed, Status: model.DeviationActive,
			FirstSeenAt: track.mismatchSince, RaisedAt: now, LastSeenAt: now,
		}
		t.active[command.ID] = alarm
		raised = append(raised, *alarm)
	}
	for commandID, alarm := range t.active {
		if alarm.ChamberID != chamberID || pendingIDs[commandID] {
			continue
		}
		delete(t.tracks, commandID)
		resolution := "command settled"
		if command, ok := t.commands.Get(commandID); !ok || command.State == model.CommandCancelled {
			resolution = "command cancelled"
		}
		if direction, ok := alarm.Device.Direction(); ok && positions[direction] == alarm.Target {
			resolution = "position matched"
		}
		resolved = append(resolved, t.resolveLocked(alarm, now, resolution))
	}
	t.mu.Unlock()

	for _, alarm := range raised {
		if _, err := t.recorder.Record(ctx, alarm.ChamberID, model.EventDeviationRaised, alarm.CycleID, alarm.Generation, alarm); err != nil {
			return err
		}
	}
	for _, alarm := range resolved {
		if _, err := t.recorder.Record(ctx, alarm.ChamberID, model.EventDeviationResolved, alarm.CycleID, alarm.Generation, alarm); err != nil {
			return err
		}
	}
	return nil
}

// Confirmable verifies that current device feedback confirms the command's
// target position. Commands without an observable position are always
// confirmable; a mismatch refreshes deviation tracking and fails.
func (t *Tracker) Confirmable(ctx context.Context, command model.DeviceCommand) error {
	direction, ok := command.Device.Direction()
	if !ok {
		return nil
	}
	target, ok := command.TargetPosition()
	if !ok {
		return nil
	}
	observed := t.state.Position(command.ChamberID, direction).Position
	if observed == target {
		return nil
	}
	if err := t.Assess(ctx, command.ChamberID); err != nil {
		return err
	}
	return fmt.Errorf("device feedback for %s is %q, command target %q not confirmed", command.Device, observed, target)
}

// List returns alarms for a chamber, active first, then most recent.
func (t *Tracker) List(chamberID string) []model.DeviationAlarm {
	t.mu.Lock()
	defer t.mu.Unlock()
	var result []model.DeviationAlarm
	for _, alarm := range t.active {
		if alarm.ChamberID == chamberID {
			result = append(result, copyAlarm(*alarm))
		}
	}
	for _, alarm := range t.resolved {
		if alarm.ChamberID == chamberID {
			result = append(result, copyAlarm(alarm))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if (result[i].Status == model.DeviationActive) != (result[j].Status == model.DeviationActive) {
			return result[i].Status == model.DeviationActive
		}
		return result[i].FirstSeenAt.After(result[j].FirstSeenAt)
	})
	return result
}

// resolveLocked closes an active alarm and moves it to the resolved log.
// The tracker mutex must be held.
func (t *Tracker) resolveLocked(alarm *model.DeviationAlarm, now time.Time, resolution string) model.DeviationAlarm {
	alarm.Status = model.DeviationResolved
	alarm.Resolution = resolution
	alarm.LastSeenAt = now
	resolvedAt := now
	alarm.ResolvedAt = &resolvedAt
	delete(t.active, alarm.CommandID)
	stored := copyAlarm(*alarm)
	t.resolved = append(t.resolved, stored)
	return stored
}

func copyAlarm(alarm model.DeviationAlarm) model.DeviationAlarm {
	if alarm.ResolvedAt != nil {
		resolvedAt := *alarm.ResolvedAt
		alarm.ResolvedAt = &resolvedAt
	}
	return alarm
}
