package gate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// ChangeSink is notified after gate command or observation state changes.
type ChangeSink interface {
	OnGateChange(chamberID string, direction model.Direction)
}

// Controller links physical observations, command lifecycle and the journal.
type Controller struct {
	state    *State
	commands *Commands
	recorder *journal.Recorder
	sink     ChangeSink
	now      func() time.Time
}

func NewController(state *State, commands *Commands, recorder *journal.Recorder, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{state: state, commands: commands, recorder: recorder, now: now}
}

// BindChangeSink attaches a watcher (for example deviation monitoring) to gate changes.
func (c *Controller) BindChangeSink(sink ChangeSink) { c.sink = sink }

func (c *Controller) notify(chamberID string, direction model.Direction) {
	if c.sink != nil {
		c.sink.OnGateChange(chamberID, direction)
	}
}

func gateDirection(device model.DeviceKind) (model.Direction, bool) {
	switch device {
	case model.DeviceUpstreamGate:
		return model.DirectionUpstream, true
	case model.DeviceDownstreamGate:
		return model.DirectionDownstream, true
	default:
		return "", false
	}
}

func targetPosition(action string) model.GatePosition {
	if action == "open" {
		return model.GateOpen
	}
	return model.GateClosed
}

func (c *Controller) Request(ctx context.Context, cycle model.TransitCycle, direction model.Direction, open bool) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	device := model.DeviceUpstreamGate
	if direction == model.DirectionDownstream {
		device = model.DeviceDownstreamGate
	}
	action := "close"
	position := model.GateClosing
	if open {
		action = "open"
		position = model.GateOpening
	}
	command, err := c.commands.Issue(cycle, device, action, 0, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := c.state.Observe(cycle.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	if _, err := c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command); err != nil {
		return model.DeviceCommand{}, err
	}
	c.notify(cycle.ChamberID, direction)
	return command, nil
}

func (c *Controller) Confirm(ctx context.Context, ack model.DeviceAck) (model.DeviceCommand, error) {
	command, ok := c.commands.Get(ack.CommandID)
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", ack.CommandID)
	}
	if command.ChamberID != ack.ChamberID {
		return model.DeviceCommand{}, fmt.Errorf("command %s belongs to another chamber", ack.CommandID)
	}
	direction, ok := gateDirection(command.Device)
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("command %s does not target a gate", ack.CommandID)
	}
	// A command settles only when the independently observed position reaches
	// the target. Acks must never fabricate the observation themselves.
	target := targetPosition(command.Action)
	observed := c.state.Position(command.ChamberID, direction).Position
	if observed != target {
		return model.DeviceCommand{}, fmt.Errorf("observed gate %s position %s does not match target %s", direction, observed, target)
	}
	settled, err := c.commands.Settle(ack, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if _, err := c.recorder.Record(ctx, settled.ChamberID, model.EventCommandSettled, settled.CycleID, settled.Generation, settled); err != nil {
		return model.DeviceCommand{}, err
	}
	c.notify(settled.ChamberID, direction)
	return settled, nil
}

func (c *Controller) Observe(chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	if err := c.state.Observe(chamberID, direction, position, c.now()); err != nil {
		return err
	}
	c.notify(chamberID, direction)
	return nil
}

func (c *Controller) State() *State       { return c.state }
func (c *Controller) Commands() *Commands { return c.commands }
