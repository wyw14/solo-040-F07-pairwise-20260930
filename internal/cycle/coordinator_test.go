package cycle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/deviation"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func TestUnmatchedCommandDoesNotAdvanceStage(t *testing.T) {
	current := time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)
	now := func() time.Time { return current }
	ctx := context.Background()

	state := gate.NewState("chamber-1")
	commands := gate.NewCommands()
	recorder := journal.NewRecorder(journal.NewMemoryStore(), now)
	gates := gate.NewController(state, commands, recorder, now)
	hydraulicsController := hydraulics.NewController(commands, recorder, hydraulics.NewRamp(time.Millisecond, now), now)
	tracker := deviation.NewTracker(state, commands, recorder, 30*time.Second, now)
	coordinator := NewCoordinator(gates, hydraulicsController, tracker, recorder, now)

	vessel, err := model.NewVessel("River Star", "RS-204", 86)
	if err != nil {
		t.Fatalf("vessel: %v", err)
	}
	plan, err := model.NewConvoyPlan("chamber-1", model.DirectionUpstream, []model.Vessel{vessel}, current)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cycleValue, err := coordinator.Start(ctx, plan)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	command, err := gates.Request(ctx, cycleValue, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	ack := model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: current,
	}

	// Feedback still reports "opening": the ack must not move the stage.
	if err := coordinator.ApplyGateAck(ctx, ack); err == nil || !strings.Contains(err.Error(), "not confirmed") {
		t.Fatalf("expected unconfirmed-target error, got %v", err)
	}
	if got, _ := coordinator.Current("chamber-1"); got.Stage != model.StagePlanned {
		t.Fatalf("stage must stay planned while feedback mismatches, got %s", got.Stage)
	}
	if stored, _ := commands.Get(command.ID); stored.State == model.CommandSettled {
		t.Fatal("unmatched command must not settle")
	}

	// Once feedback confirms the target, the same ack settles and advances.
	if err := state.Observe("chamber-1", model.DirectionUpstream, model.GateOpen, current); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if err := tracker.Assess(ctx, "chamber-1"); err != nil {
		t.Fatalf("assess: %v", err)
	}
	if err := coordinator.ApplyGateAck(ctx, ack); err != nil {
		t.Fatalf("ack with matching feedback should succeed: %v", err)
	}
	if got, _ := coordinator.Current("chamber-1"); got.Stage != model.StageAdmitting {
		t.Fatalf("stage should advance after confirmation, got %s", got.Stage)
	}
}

func TestDeviationAlarmBlocksAckUntilCleared(t *testing.T) {
	current := time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)
	now := func() time.Time { return current }
	ctx := context.Background()

	state := gate.NewState("chamber-1")
	commands := gate.NewCommands()
	recorder := journal.NewRecorder(journal.NewMemoryStore(), now)
	gates := gate.NewController(state, commands, recorder, now)
	hydraulicsController := hydraulics.NewController(commands, recorder, hydraulics.NewRamp(time.Millisecond, now), now)
	tracker := deviation.NewTracker(state, commands, recorder, 30*time.Second, now)
	coordinator := NewCoordinator(gates, hydraulicsController, tracker, recorder, now)

	vessel, err := model.NewVessel("River Star", "RS-204", 86)
	if err != nil {
		t.Fatalf("vessel: %v", err)
	}
	plan, err := model.NewConvoyPlan("chamber-1", model.DirectionUpstream, []model.Vessel{vessel}, current)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cycleValue, err := coordinator.Start(ctx, plan)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	command, err := gates.Request(ctx, cycleValue, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	// Let the mismatch outlive the threshold so an alarm is active.
	current = current.Add(31 * time.Second)
	if err := tracker.Assess(ctx, "chamber-1"); err != nil {
		t.Fatalf("assess: %v", err)
	}
	if alarms := tracker.List("chamber-1"); len(alarms) != 1 {
		t.Fatalf("expected one active deviation alarm, got %d", len(alarms))
	}

	ack := model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: current,
	}
	if err := coordinator.ApplyGateAck(ctx, ack); err == nil {
		t.Fatal("active deviation must block stage advancement")
	}

	// Matching feedback auto-resolves the alarm and unblocks the ack.
	if err := state.Observe("chamber-1", model.DirectionUpstream, model.GateOpen, current); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if err := tracker.Assess(ctx, "chamber-1"); err != nil {
		t.Fatalf("assess: %v", err)
	}
	for _, alarm := range tracker.List("chamber-1") {
		if alarm.Status != model.DeviationResolved || alarm.Resolution != "position matched" {
			t.Fatalf("alarm should auto-resolve on match: %+v", alarm)
		}
	}
	if err := coordinator.ApplyGateAck(ctx, ack); err != nil {
		t.Fatalf("ack should succeed once deviation cleared: %v", err)
	}
	if got, _ := coordinator.Current("chamber-1"); got.Stage != model.StageAdmitting {
		t.Fatalf("stage should advance after deviation cleared, got %s", got.Stage)
	}
}
