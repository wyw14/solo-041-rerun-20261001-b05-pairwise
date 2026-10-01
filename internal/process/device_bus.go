package process

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/wyw14/cry-97/internal/model"
)

// ErrStaleCommand marks a device command that lost the ordering race against a
// newer command — or an emergency generation switch — already applied on the
// bus. Stale commands are dropped so an old setting can never override a newer
// one; publishers may treat the error as a conflict rather than a failure.
var ErrStaleCommand = errors.New("device command is stale")

type DeviceBus struct {
	mu       sync.RWMutex
	commands map[string]model.DeviceCommand
	// lineFloor is the lowest generation a line still accepts. An emergency
	// isolation raises it so late commands from before the isolation are
	// rejected even for devices that have no entry yet.
	lineFloor map[model.LineID]uint64
}

func NewDeviceBus() *DeviceBus {
	return &DeviceBus{
		commands:  make(map[string]model.DeviceCommand),
		lineFloor: make(map[model.LineID]uint64),
	}
}

func (b *DeviceBus) Publish(ctx context.Context, command model.DeviceCommand) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if command.DeviceID == "" || command.Generation == 0 {
		return errors.New("device command is incomplete")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if floor := b.lineFloor[command.LineID]; command.Generation < floor {
		return fmt.Errorf("%w: line %s is isolated at generation %d", ErrStaleCommand, command.LineID, floor)
	}
	current, exists := b.commands[command.DeviceID]
	if exists {
		if current.ID == command.ID && current.Generation == command.Generation && current.Sequence == command.Sequence {
			// Duplicate delivery of an already applied command: acknowledge
			// the retry without driving the device again.
			return nil
		}
		if !commandOrdersAfter(command, current) {
			return fmt.Errorf("%w: device %s already applied generation %d sequence %d",
				ErrStaleCommand, command.DeviceID, current.Generation, current.Sequence)
		}
	}
	b.commands[command.DeviceID] = command
	return nil
}

// commandOrdersAfter reports whether next is newer than current. Generation
// dominates; the journal sequence orders commands inside one generation.
func commandOrdersAfter(next, current model.DeviceCommand) bool {
	if next.Generation != current.Generation {
		return next.Generation > current.Generation
	}
	return next.Sequence > current.Sequence
}

func (b *DeviceBus) All() []model.DeviceCommand {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]model.DeviceCommand, 0, len(b.commands))
	for _, command := range b.commands {
		result = append(result, command)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].IssuedAt.Before(result[j].IssuedAt) })
	return result
}

// StopLine isolates a line at a new generation: every known command of the
// line is replaced by a stop, and the line floor is raised so late commands
// from older generations are rejected on arrival. sequence is the journal
// sequence of the emergency event, keeping the ordering key meaningful for
// commands issued after the isolation.
func (b *DeviceBus) StopLine(lineID model.LineID, generation, sequence uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation > b.lineFloor[lineID] {
		b.lineFloor[lineID] = generation
	}
	for id, command := range b.commands {
		if command.LineID != lineID {
			continue
		}
		command.Generation = generation
		command.Sequence = sequence
		command.Action = "stop"
		command.Setpoint = 0
		b.commands[id] = command
	}
}
