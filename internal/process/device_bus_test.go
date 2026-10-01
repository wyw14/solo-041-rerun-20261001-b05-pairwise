package process

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func testCommand(deviceID string, generation, sequence uint64, setpoint float64) model.DeviceCommand {
	return model.DeviceCommand{
		ID: uuid.New(), LineID: "line-1", BatchID: uuid.New(),
		DeviceID: deviceID, Kind: model.DeviceDoser, Generation: generation,
		Sequence: sequence, Action: "set", Setpoint: setpoint,
		IssuedAt: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC),
	}
}

func busCommand(t *testing.T, bus *DeviceBus, deviceID string) model.DeviceCommand {
	t.Helper()
	for _, command := range bus.All() {
		if command.DeviceID == deviceID {
			return command
		}
	}
	t.Fatalf("device %s has no command on the bus", deviceID)
	return model.DeviceCommand{}
}

// The reported bug: rate 2 is set, then rate 4; the old command arrives late
// (concurrent retry) and must not push the bus back to 2.
func TestDeviceBusLateCommandCannotOverrideNewerSameGeneration(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	older := testCommand("doser-line-1-pac", 1, 1, 2)
	newer := testCommand("doser-line-1-pac", 1, 2, 4)
	if err := bus.Publish(ctx, older); err != nil {
		t.Fatalf("publish older command: %v", err)
	}
	if err := bus.Publish(ctx, newer); err != nil {
		t.Fatalf("publish newer command: %v", err)
	}
	if err := bus.Publish(ctx, older); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("late old command: expected ErrStaleCommand, got %v", err)
	}
	if got := busCommand(t, bus, "doser-line-1-pac").Setpoint; got != 4 {
		t.Fatalf("late old command overrode newer setpoint: got %v, want 4", got)
	}
}

// Out-of-order first delivery: the newer command lands before the older one.
func TestDeviceBusOutOfOrderDeliveryKeepsNewest(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	older := testCommand("doser-line-1-pac", 1, 1, 2)
	newer := testCommand("doser-line-1-pac", 1, 2, 4)
	if err := bus.Publish(ctx, newer); err != nil {
		t.Fatalf("publish newer command: %v", err)
	}
	if err := bus.Publish(ctx, older); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("late old command: expected ErrStaleCommand, got %v", err)
	}
	if got := busCommand(t, bus, "doser-line-1-pac").Setpoint; got != 4 {
		t.Fatalf("bus regressed to old setpoint: got %v, want 4", got)
	}
}

// A legitimate retry of the command that is currently applied is acknowledged
// without driving the device again.
func TestDeviceBusDuplicateRetryIsIdempotent(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	command := testCommand("doser-line-1-pac", 1, 2, 4)
	if err := bus.Publish(ctx, command); err != nil {
		t.Fatalf("publish command: %v", err)
	}
	retry := command
	retry.IssuedAt = command.IssuedAt.Add(time.Minute) // late retry copy
	if err := bus.Publish(ctx, retry); err != nil {
		t.Fatalf("legitimate retry must be acknowledged, got %v", err)
	}
	stored := busCommand(t, bus, "doser-line-1-pac")
	if !stored.IssuedAt.Equal(command.IssuedAt) {
		t.Fatalf("retry re-drove the device: stored IssuedAt %v, want %v", stored.IssuedAt, command.IssuedAt)
	}
}

// Two different commands cannot share one ordering key: the second one is
// rejected instead of silently replacing the first.
func TestDeviceBusRejectsSameSequenceDifferentCommand(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	first := testCommand("doser-line-1-pac", 1, 2, 4)
	second := testCommand("doser-line-1-pac", 1, 2, 6)
	if err := bus.Publish(ctx, first); err != nil {
		t.Fatalf("publish first command: %v", err)
	}
	if err := bus.Publish(ctx, second); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("conflicting command: expected ErrStaleCommand, got %v", err)
	}
	if got := busCommand(t, bus, "doser-line-1-pac").Setpoint; got != 4 {
		t.Fatalf("conflicting command overrode applied setpoint: got %v, want 4", got)
	}
}

func TestDeviceBusRejectsOlderGeneration(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	if err := bus.Publish(ctx, testCommand("doser-line-1-pac", 2, 1, 4)); err != nil {
		t.Fatalf("publish generation 2 command: %v", err)
	}
	if err := bus.Publish(ctx, testCommand("doser-line-1-pac", 1, 5, 2)); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("older generation: expected ErrStaleCommand, got %v", err)
	}
}

// After an emergency isolation, commands from the old generation are rejected
// — including devices that never had a command on the bus — and commands from
// the new generation are ordered against the emergency sequence.
func TestDeviceBusEmergencyFloor(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	applied := testCommand("doser-line-1-pac", 1, 3, 4)
	if err := bus.Publish(ctx, applied); err != nil {
		t.Fatalf("publish command: %v", err)
	}
	bus.StopLine("line-1", 2, 10)

	stopped := busCommand(t, bus, "doser-line-1-pac")
	if stopped.Action != "stop" || stopped.Generation != 2 || stopped.Setpoint != 0 {
		t.Fatalf("command not stopped: %+v", stopped)
	}
	if err := bus.Publish(ctx, applied); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("pre-emergency command: expected ErrStaleCommand, got %v", err)
	}
	otherDevice := testCommand("doser-line-1-naoh", 1, 4, 3)
	if err := bus.Publish(ctx, otherDevice); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("pre-emergency command on new device: expected ErrStaleCommand, got %v", err)
	}
	beforeEmergencySeq := testCommand("doser-line-1-pac", 2, 5, 6)
	if err := bus.Publish(ctx, beforeEmergencySeq); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("command older than emergency sequence: expected ErrStaleCommand, got %v", err)
	}
	afterEmergency := testCommand("doser-line-1-pac", 2, 11, 6)
	if err := bus.Publish(ctx, afterEmergency); err != nil {
		t.Fatalf("new generation command must be accepted: %v", err)
	}
	if got := busCommand(t, bus, "doser-line-1-pac").Setpoint; got != 6 {
		t.Fatalf("new generation command not applied: got %v, want 6", got)
	}
}

// The floor is per line: isolating one line must not block another.
func TestDeviceBusEmergencyFloorIsPerLine(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	bus.StopLine("line-1", 2, 10)
	other := testCommand("doser-line-2-pac", 1, 1, 3)
	other.LineID = "line-2"
	if err := bus.Publish(ctx, other); err != nil {
		t.Fatalf("other line must stay unaffected: %v", err)
	}
}
