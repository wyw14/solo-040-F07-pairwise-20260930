package gate

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func testCycle(now time.Time) model.TransitCycle {
	cycle, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionUpstream, 1, now)
	if err != nil {
		panic(err)
	}
	return cycle
}

func newConfirmController(now func() time.Time) (*Controller, *Commands) {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, now)
	state := NewState("chamber-1")
	commands := NewCommands()
	return NewController(state, commands, recorder, now), commands
}

func ackFor(command model.DeviceCommand) model.DeviceAck {
	return model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: command.UpdatedAt,
	}
}

// A command must not settle while the observed position is short of target.
func TestConfirmRejectsPositionMismatch(t *testing.T) {
	current := time.Unix(1_000, 0).UTC()
	controller, commands := newConfirmController(func() time.Time { return current })
	cycle := testCycle(current)

	command, err := controller.Request(context.Background(), cycle, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, err := controller.Confirm(context.Background(), ackFor(command)); err == nil {
		t.Fatal("expected confirm to fail while gate is still opening")
	}
	stored, _ := commands.Get(command.ID)
	if stored.State.Terminal() {
		t.Fatalf("command must remain non-terminal on mismatch, got %s", stored.State)
	}

	// Physical feedback reaches the target; only then may the command settle.
	if err := controller.Observe("chamber-1", model.DirectionUpstream, model.GateOpen); err != nil {
		t.Fatalf("observe: %v", err)
	}
	settled, err := controller.Confirm(context.Background(), ackFor(command))
	if err != nil {
		t.Fatalf("confirm after match: %v", err)
	}
	if settled.State != model.CommandSettled {
		t.Fatalf("state = %s, want settled", settled.State)
	}
	if position := controller.State().Position("chamber-1", model.DirectionUpstream).Position; position != model.GateOpen {
		t.Fatalf("observed position = %s, want open", position)
	}
}

// Confirm must never substitute the target for a missing field observation.
func TestConfirmDoesNotFabricateObservation(t *testing.T) {
	current := time.Unix(1_000, 0).UTC()
	controller, _ := newConfirmController(func() time.Time { return current })
	cycle := testCycle(current)

	command, err := controller.Request(context.Background(), cycle, model.DirectionDownstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, err := controller.Confirm(context.Background(), ackFor(command)); err == nil {
		t.Fatal("downstream gate never observed at target, confirm must fail")
	}
	if position := controller.State().Position("chamber-1", model.DirectionDownstream).Position; position != model.GateOpening {
		t.Fatalf("position = %s, confirm must not overwrite observation", position)
	}
}
