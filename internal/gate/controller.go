package gate

import (
	"context"
	"errors"
	"time"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Controller links physical observations, command lifecycle and the journal.
type Controller struct {
	state    *State
	commands *Commands
	recorder *journal.Recorder
	now      func() time.Time
}

func NewController(state *State, commands *Commands, recorder *journal.Recorder, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{state: state, commands: commands, recorder: recorder, now: now}
}

func (c *Controller) Request(ctx context.Context, cycle model.TransitCycle, direction model.Direction, open bool) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	device := model.DeviceUpstreamGate
	if direction == model.DirectionDownstream {
		device = model.DeviceDownstreamGate
	}
	action := model.ActionClose
	position := model.GateClosing
	if open {
		action = model.ActionOpen
		position = model.GateOpening
	}
	command, err := c.commands.Issue(cycle, device, action, 0, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := c.state.Observe(cycle.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	_, err = c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command)
	return command, err
}

func (c *Controller) Confirm(ctx context.Context, ack model.DeviceAck) (model.DeviceCommand, error) {
	command, err := c.commands.Settle(ack, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	direction := model.DirectionUpstream
	if command.Device == model.DeviceDownstreamGate {
		direction = model.DirectionDownstream
	}
	position := model.GateClosed
	if command.Action == model.ActionOpen {
		position = model.GateOpen
	}
	if err := c.state.Observe(command.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCommandSettled, command.CycleID, command.Generation, command)
	return command, err
}

func (c *Controller) Observe(chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	return c.state.Observe(chamberID, direction, position, c.now())
}

func (c *Controller) State() *State       { return c.state }
func (c *Controller) Commands() *Commands { return c.commands }
