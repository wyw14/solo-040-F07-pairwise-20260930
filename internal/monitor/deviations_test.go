package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type deviationFixture struct {
	now      time.Time
	commands *gate.Commands
	state    *gate.State
	store    journal.Store
	watch    *DeviationWatch
}

func newDeviationFixture(t *testing.T) *deviationFixture {
	t.Helper()
	base := time.Unix(10_000, 0).UTC()
	f := &deviationFixture{now: base}
	clock := func() time.Time { return f.now }
	f.store = journal.NewMemoryStore()
	recorder := journal.NewRecorder(f.store, clock)
	f.commands = gate.NewCommands()
	f.state = gate.NewState("chamber-1", "chamber-2")
	f.watch = NewDeviationWatch([]string{"chamber-1", "chamber-2"}, f.commands, f.state, recorder, 30*time.Second, clock)
	return f
}

func (f *deviationFixture) cycle(gen uint64) model.TransitCycle {
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, gen, f.now)
	if err != nil {
		panic(err)
	}
	return cycle
}

func (f *deviationFixture) issue(device model.DeviceKind, action string) model.DeviceCommand {
	command, err := f.commands.Issue(f.cycle(1), device, action, 0, f.now)
	if err != nil {
		panic(err)
	}
	return command
}

func (f *deviationFixture) eventCount(kind model.EventType) int {
	events, err := f.store.Events(context.Background(), "chamber-1", 0)
	if err != nil {
		panic(err)
	}
	count := 0
	for _, event := range events {
		if event.Type == kind {
			count++
		}
	}
	return count
}

func findDeviation(items []GateDeviation, commandID uuid.UUID) (GateDeviation, bool) {
	for _, item := range items {
		if item.CommandID == commandID {
			return item, true
		}
	}
	return GateDeviation{}, false
}

func activeCount(items []GateDeviation) int {
	count := 0
	for _, item := range items {
		if item.Status == DeviationActive {
			count++
		}
	}
	return count
}

func TestDeviationRaisesAfterThresholdAndDedupes(t *testing.T) {
	f := newDeviationFixture(t)
	// Default observed position is closed; commanding open stays unmatched.
	command := f.issue(model.DeviceUpstreamGate, "open")

	f.now = f.now.Add(10 * time.Second)
	if active := f.watch.Evaluate("chamber-1"); len(active) != 0 {
		t.Fatalf("within threshold: active = %d, want 0", len(active))
	}

	f.now = f.now.Add(21 * time.Second) // 31s since issue
	if active := f.watch.Evaluate("chamber-1"); len(active) != 1 {
		t.Fatalf("after threshold: active = %d, want 1", len(active))
	}
	if raised := f.eventCount(model.EventGateDeviationRaised); raised != 1 {
		t.Fatalf("raised events = %d, want 1", raised)
	}

	first := f.watch.List()
	alert, ok := findDeviation(first, command.ID)
	if !ok || alert.Status != DeviationActive {
		t.Fatal("expected one active deviation for command")
	}

	// Repeated unmatched observations refresh the same alert, never a new one.
	f.now = f.now.Add(5 * time.Second)
	if err := f.state.Observe("chamber-1", model.DirectionUpstream, model.GateClosing, f.now); err != nil {
		t.Fatalf("observe: %v", err)
	}
	f.watch.Evaluate("chamber-1")
	if raised := f.eventCount(model.EventGateDeviationRaised); raised != 1 {
		t.Fatalf("repeated observation raised %d events, want still 1", raised)
	}
	again := f.watch.List()
	if activeCount(again) != 1 {
		t.Fatalf("active after repeat = %d, want 1", activeCount(again))
	}
	refreshed, _ := findDeviation(again, command.ID)
	if refreshed.ID != alert.ID || refreshed.ActualPosition != model.GateClosing {
		t.Fatal("repeated observation must refresh the same alert in place")
	}
}

func TestDeviationResolvesOnMatch(t *testing.T) {
	f := newDeviationFixture(t)
	command := f.issue(model.DeviceUpstreamGate, "open")
	f.now = f.now.Add(31 * time.Second)
	f.watch.Evaluate("chamber-1")

	if err := f.state.Observe("chamber-1", model.DirectionUpstream, model.GateOpen, f.now); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if active := f.watch.Evaluate("chamber-1"); len(active) != 0 {
		t.Fatalf("matched: active = %d, want 0", len(active))
	}
	items := f.watch.List()
	if activeCount(items) != 0 {
		t.Fatal("matched position must clear the active deviation")
	}
	resolved, ok := findDeviation(items, command.ID)
	if !ok || resolved.Status != DeviationResolved || resolved.Reason != "matched" {
		t.Fatal("expected resolved deviation with reason matched")
	}
	if f.eventCount(model.EventGateDeviationResolved) != 1 {
		t.Fatal("expected one resolved event")
	}
}

func TestDeviationResolvesOnCancelAndAcknowledge(t *testing.T) {
	f := newDeviationFixture(t)
	command := f.issue(model.DeviceUpstreamGate, "close") // observed closed -> actually matches!
	// Force an unmatched state by observing the gate open.
	if err := f.state.Observe("chamber-1", model.DirectionUpstream, model.GateOpen, f.now); err != nil {
		t.Fatalf("observe: %v", err)
	}
	f.now = f.now.Add(31 * time.Second)
	f.watch.Evaluate("chamber-1")

	acknowledged, err := f.watch.Acknowledge("chamber-1", command.ID)
	if err != nil || !acknowledged.Acknowledged || acknowledged.AcknowledgedAt == nil {
		t.Fatalf("acknowledge failed: %v acknowledged=%v", err, acknowledged.Acknowledged)
	}
	if f.eventCount(model.EventGateDeviationAcknowledged) != 1 {
		t.Fatal("expected one acknowledged event")
	}

	cancelled := f.commands.CancelChamber("chamber-1", f.now)
	if len(cancelled) != 1 {
		t.Fatalf("cancelled commands = %d, want 1", len(cancelled))
	}
	if active := f.watch.Evaluate("chamber-1"); len(active) != 0 {
		t.Fatalf("after cancel: active = %d, want 0", len(active))
	}
	resolved, ok := findDeviation(f.watch.List(), command.ID)
	if !ok || resolved.Reason != "command cancelled" {
		t.Fatalf("expected cancel resolution, got %+v", resolved)
	}
}

func TestDeviationResolvesWhenSuperseded(t *testing.T) {
	f := newDeviationFixture(t)
	first := f.issue(model.DeviceUpstreamGate, "open")
	f.now = f.now.Add(31 * time.Second)
	f.watch.Evaluate("chamber-1")

	// A newer command for the same device supersedes the still-open alert.
	replacement := f.issue(model.DeviceUpstreamGate, "open")
	if active := f.watch.Evaluate("chamber-1"); len(active) != 0 {
		t.Fatalf("fresh replacement within threshold: active = %d, want 0", len(active))
	}
	resolved, ok := findDeviation(f.watch.List(), first.ID)
	if !ok || resolved.Reason != "superseded" {
		t.Fatalf("expected first deviation superseded, got %+v", resolved)
	}
	if _, stillThere := findDeviation(f.watch.List(), replacement.ID); stillThere {
		t.Fatal("replacement command below threshold must not raise yet")
	}
}

func TestChangeSinkRaisesOnObservation(t *testing.T) {
	f := newDeviationFixture(t)
	controller := gate.NewController(f.state, f.commands, journal.NewRecorder(f.store, func() time.Time { return f.now }), func() time.Time { return f.now })
	controller.BindChangeSink(f.watch)

	cycle := f.cycle(2)
	if _, err := controller.Request(context.Background(), cycle, model.DirectionDownstream, true); err != nil {
		t.Fatalf("request: %v", err)
	}
	f.now = f.now.Add(31 * time.Second)
	if err := controller.Observe("chamber-1", model.DirectionDownstream, model.GateClosed); err != nil {
		t.Fatalf("observe: %v", err)
	}
	items := f.watch.List()
	alert, ok := findDeviation(items, controller.Commands().Pending("chamber-1")[0].ID)
	if !ok || alert.Status != DeviationActive || alert.Device != model.DeviceDownstreamGate {
		t.Fatalf("expected active downstream deviation via sink, got %+v", items)
	}
}
