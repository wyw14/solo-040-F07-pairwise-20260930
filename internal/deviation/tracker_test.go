package deviation

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

const testChamber = "chamber-1"

type fixture struct {
	current  time.Time
	state    *gate.State
	commands *gate.Commands
	store    *journal.MemoryStore
	tracker  *Tracker
}

func newFixture(threshold time.Duration) *fixture {
	f := &fixture{current: time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)}
	now := func() time.Time { return f.current }
	f.state = gate.NewState(testChamber)
	f.commands = gate.NewCommands()
	f.store = journal.NewMemoryStore()
	f.tracker = NewTracker(f.state, f.commands, journal.NewRecorder(f.store, now), threshold, now)
	return f
}

func (f *fixture) advance(delta time.Duration) { f.current = f.current.Add(delta) }

func (f *fixture) issue(device model.DeviceKind, action string) model.DeviceCommand {
	cycle, err := model.NewTransitCycle(testChamber, uuid.New(), model.DirectionUpstream, 1, f.current)
	if err != nil {
		panic(err)
	}
	command, err := f.commands.Issue(cycle, device, action, 0, f.current)
	if err != nil {
		panic(err)
	}
	return command
}

func (f *fixture) observe(position model.GatePosition) {
	if err := f.state.Observe(testChamber, model.DirectionUpstream, position, f.current); err != nil {
		panic(err)
	}
}

func (f *fixture) assess(t *testing.T) {
	t.Helper()
	if err := f.tracker.Assess(context.Background(), testChamber); err != nil {
		t.Fatalf("assess: %v", err)
	}
}

func (f *fixture) events(t *testing.T, kind model.EventType) int {
	t.Helper()
	events, err := f.store.Events(context.Background(), testChamber, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	count := 0
	for _, event := range events {
		if event.Type == kind {
			count++
		}
	}
	return count
}

func activeAlarms(alarms []model.DeviationAlarm) []model.DeviationAlarm {
	var result []model.DeviationAlarm
	for _, alarm := range alarms {
		if alarm.Status == model.DeviationActive {
			result = append(result, alarm)
		}
	}
	return result
}

func resolvedAlarms(alarms []model.DeviationAlarm) []model.DeviationAlarm {
	var result []model.DeviationAlarm
	for _, alarm := range alarms {
		if alarm.Status == model.DeviationResolved {
			result = append(result, alarm)
		}
	}
	return result
}

func TestMismatchBelowThresholdRaisesNoAlarm(t *testing.T) {
	f := newFixture(30 * time.Second)
	f.issue(model.DeviceUpstreamGate, model.ActionOpen)
	f.observe(model.GateOpening)
	f.advance(10 * time.Second)
	f.assess(t)
	if alarms := f.tracker.List(testChamber); len(alarms) != 0 {
		t.Fatalf("expected no alarms below threshold, got %d", len(alarms))
	}
}

func TestRepeatedObservationsKeepSingleAlarm(t *testing.T) {
	f := newFixture(30 * time.Second)
	start := f.current
	command := f.issue(model.DeviceUpstreamGate, model.ActionOpen)
	f.observe(model.GateOpening)

	f.advance(31 * time.Second)
	f.assess(t)
	alarms := activeAlarms(f.tracker.List(testChamber))
	if len(alarms) != 1 {
		t.Fatalf("expected one active alarm, got %d", len(alarms))
	}
	if !alarms[0].FirstSeenAt.Equal(start) {
		t.Fatalf("deviation should date back to command issuance %s, got %s", start, alarms[0].FirstSeenAt)
	}
	if alarms[0].CommandID != command.ID || alarms[0].Target != model.GateOpen || alarms[0].Observed != model.GateOpening {
		t.Fatalf("alarm does not reflect command and observation: %+v", alarms[0])

	}

	// Repeated mismatch observations refresh the same alarm instead of adding new ones.
	f.advance(9 * time.Second)
	f.observe(model.GateOpening)
	f.assess(t)
	f.advance(5 * time.Second)
	f.observe(model.GateClosed)
	f.assess(t)

	alarms = activeAlarms(f.tracker.List(testChamber))
	if len(alarms) != 1 {
		t.Fatalf("repeated observations must not add alarms, got %d", len(alarms))
	}
	if !alarms[0].LastSeenAt.Equal(f.current) || alarms[0].Observed != model.GateClosed {
		t.Fatalf("alarm should track latest observation: %+v", alarms[0])
	}
	if got := f.events(t, model.EventDeviationRaised); got != 1 {
		t.Fatalf("expected exactly one raised event, got %d", got)
	}
}

func TestMatchingObservationResolvesAlarm(t *testing.T) {
	f := newFixture(30 * time.Second)
	f.issue(model.DeviceUpstreamGate, model.ActionOpen)
	f.observe(model.GateOpening)
	f.advance(31 * time.Second)
	f.assess(t)

	f.advance(14 * time.Second)
	f.observe(model.GateOpen)
	f.assess(t)

	alarms := f.tracker.List(testChamber)
	if len(activeAlarms(alarms)) != 0 {
		t.Fatalf("matching observation should clear active alarms: %+v", alarms)
	}
	resolved := resolvedAlarms(alarms)
	if len(resolved) != 1 {
		t.Fatalf("expected one resolved alarm, got %d", len(resolved))
	}
	if resolved[0].Resolution != "position matched" {
		t.Fatalf("unexpected resolution %q", resolved[0].Resolution)
	}
	if duration := resolved[0].Duration(f.current); duration != 45*time.Second {
		t.Fatalf("episode duration should be 45s, got %s", duration)
	}
	if got := f.events(t, model.EventDeviationResolved); got != 1 {
		t.Fatalf("expected one resolved event, got %d", got)
	}
}

func TestMismatchAfterMatchStartsNewEpisode(t *testing.T) {
	f := newFixture(30 * time.Second)
	f.issue(model.DeviceUpstreamGate, model.ActionOpen)
	f.observe(model.GateOpening)
	f.advance(31 * time.Second)
	f.assess(t)
	first := activeAlarms(f.tracker.List(testChamber))[0]

	f.observe(model.GateOpen)
	f.assess(t)

	// Feedback slips away from the target again: a fresh episode begins.
	f.advance(5 * time.Second)
	slipStart := f.current
	f.observe(model.GateClosed)
	f.assess(t)
	if alarms := activeAlarms(f.tracker.List(testChamber)); len(alarms) != 0 {
		t.Fatalf("new episode below threshold should not alarm: %+v", alarms)
	}
	f.advance(30 * time.Second)
	f.assess(t)

	alarms := activeAlarms(f.tracker.List(testChamber))
	if len(alarms) != 1 {
		t.Fatalf("expected one active alarm for the new episode, got %d", len(alarms))
	}
	if alarms[0].ID == first.ID {
		t.Fatal("new episode must raise a new alarm, not reopen the resolved one")
	}
	if !alarms[0].FirstSeenAt.Equal(slipStart) {
		t.Fatalf("new episode should start at the slip, got %s", alarms[0].FirstSeenAt)
	}
}

func TestCancelledCommandResolvesAlarm(t *testing.T) {
	f := newFixture(30 * time.Second)
	f.issue(model.DeviceUpstreamGate, model.ActionOpen)
	f.observe(model.GateClosed)
	f.advance(31 * time.Second)
	f.assess(t)
	if alarms := activeAlarms(f.tracker.List(testChamber)); len(alarms) != 1 {
		t.Fatalf("expected one active alarm, got %d", len(alarms))
	}

	f.commands.CancelChamber(testChamber, f.current)
	f.assess(t)

	resolved := resolvedAlarms(f.tracker.List(testChamber))
	if len(resolved) != 1 || resolved[0].Resolution != "command cancelled" {
		t.Fatalf("cancelled command should resolve its alarm: %+v", resolved)
	}
}

func TestConfirmable(t *testing.T) {
	f := newFixture(30 * time.Second)
	command := f.issue(model.DeviceUpstreamGate, model.ActionOpen)

	f.observe(model.GateOpening)
	if err := f.tracker.Confirmable(context.Background(), command); err == nil {
		t.Fatal("mismatched feedback must not confirm the command")
	}

	f.observe(model.GateOpen)
	if err := f.tracker.Confirmable(context.Background(), command); err != nil {
		t.Fatalf("matching feedback should confirm: %v", err)
	}

	valve := f.issue(model.DeviceFillValve, "set_opening")
	if err := f.tracker.Confirmable(context.Background(), valve); err != nil {
		t.Fatalf("devices without observable position are always confirmable: %v", err)
	}
}
