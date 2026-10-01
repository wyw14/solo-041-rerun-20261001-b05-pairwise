package process

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T, dir string) *Plant {
	t.Helper()
	clock := func() time.Time { return time.Now().UTC() }
	plant, err := NewPlant(filepath.Join(dir, "data"), []model.LineID{"line-1"}, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func startTestBatch(t *testing.T, plant *Plant, lineID model.LineID) model.Batch {
	t.Helper()
	batch, err := plant.StartBatch(context.Background(), lineID, 120, "influent")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	return batch
}

func statusCommand(t *testing.T, status LineStatus, deviceID string) model.DeviceCommand {
	t.Helper()
	for _, command := range status.Commands {
		if command.DeviceID == deviceID {
			return command
		}
	}
	t.Fatalf("device %s has no command in line status", deviceID)
	return model.DeviceCommand{}
}

// The reported incident: rate 2 then rate 4 on one batch of one line; a late
// retry of the old command must not push the device bus back to 2, and a
// legitimate retry of the current command must not re-drive the device.
func TestChangeDoseLateRetryCannotOverrideNewerRate(t *testing.T) {
	plant := newTestPlant(t, t.TempDir())
	ctx := context.Background()
	lineID := model.LineID("line-1")
	batch := startTestBatch(t, plant, lineID)

	oldCommand, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", 2)
	if err != nil {
		t.Fatalf("change dose to 2: %v", err)
	}
	newCommand, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", 4)
	if err != nil {
		t.Fatalf("change dose to 4: %v", err)
	}
	if newCommand.Sequence <= oldCommand.Sequence {
		t.Fatalf("commands are not ordered by journal sequence: %d then %d", oldCommand.Sequence, newCommand.Sequence)
	}

	if err := plant.devices.Publish(ctx, oldCommand); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("late old command: expected ErrStaleCommand, got %v", err)
	}
	if err := plant.devices.Publish(ctx, newCommand); err != nil {
		t.Fatalf("legitimate retry of current command must be acknowledged: %v", err)
	}

	status, err := plant.Status(lineID)
	if err != nil {
		t.Fatalf("line status: %v", err)
	}
	if status.Dose == nil || status.Dose.Rate != 4 {
		t.Fatalf("status page dose: got %+v, want rate 4", status.Dose)
	}
	applied := statusCommand(t, status, "doser-line-1-pac")
	if applied.Setpoint != 4 {
		t.Fatalf("device bus regressed: got setpoint %v, want 4", applied.Setpoint)
	}
	if applied.Sequence != newCommand.Sequence {
		t.Fatalf("device bus applied sequence %d, want %d", applied.Sequence, newCommand.Sequence)
	}
}

// Under concurrent changes the journal, the ledger behind the status page and
// the device bus must always agree on which dose is newest.
func TestChangeDoseConcurrentKeepsStatusAndBusConsistent(t *testing.T) {
	plant := newTestPlant(t, t.TempDir())
	ctx := context.Background()
	lineID := model.LineID("line-1")
	batch := startTestBatch(t, plant, lineID)

	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		rate := float64(2 + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", rate); err != nil {
				t.Errorf("concurrent change dose %v: %v", rate, err)
			}
		}()
	}
	wg.Wait()

	status, err := plant.Status(lineID)
	if err != nil {
		t.Fatalf("line status: %v", err)
	}
	if status.Dose == nil {
		t.Fatal("status page has no dose after concurrent changes")
	}
	applied := statusCommand(t, status, "doser-line-1-pac")
	if applied.Setpoint != status.Dose.Rate {
		t.Fatalf("status page dose %v diverges from device bus setpoint %v", status.Dose.Rate, applied.Setpoint)
	}
}

// An emergency isolation bumps the generation: late commands from the old
// generation are rejected, and the line refuses further dose changes.
func TestEmergencyIsolationRejectsPreEmergencyCommands(t *testing.T) {
	plant := newTestPlant(t, t.TempDir())
	ctx := context.Background()
	lineID := model.LineID("line-1")
	batch := startTestBatch(t, plant, lineID)

	command, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", 4)
	if err != nil {
		t.Fatalf("change dose: %v", err)
	}
	state, err := plant.EmergencyStop(ctx, lineID, "chlorine leak")
	if err != nil {
		t.Fatalf("emergency stop: %v", err)
	}
	if state.Generation != command.Generation+1 {
		t.Fatalf("emergency did not bump generation: command %d, line %d", command.Generation, state.Generation)
	}

	if err := plant.devices.Publish(ctx, command); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("pre-emergency command after isolation: expected ErrStaleCommand, got %v", err)
	}
	stopped := busCommand(t, plant.devices, "doser-line-1-pac")
	if stopped.Action != "stop" || stopped.Generation != state.Generation || stopped.Setpoint != 0 {
		t.Fatalf("device not stopped at new generation: %+v", stopped)
	}
	if _, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", 6); err == nil {
		t.Fatal("isolated line accepted a dose change")
	}
}

// Replaying the journal rebuilds the same bus state and the same latest dose
// as live operation, in journal order.
func TestRecoverLineRebuildsDosingFromJournal(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	lineID := model.LineID("line-1")

	plant := newTestPlant(t, dir)
	batch := startTestBatch(t, plant, lineID)
	if _, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", 2); err != nil {
		t.Fatalf("change dose to 2: %v", err)
	}
	latest, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", 4)
	if err != nil {
		t.Fatalf("change dose to 4: %v", err)
	}

	recovered := newTestPlant(t, dir)
	if _, err := recovered.RecoverLine(ctx, lineID); err != nil {
		t.Fatalf("recover line: %v", err)
	}
	change, ok := recovered.dosing.Latest(batch.ID)
	if !ok || change.Rate != 4 {
		t.Fatalf("recovered latest dose: got %+v, want rate 4", change)
	}
	applied := busCommand(t, recovered.devices, "doser-line-1-pac")
	if applied.Setpoint != 4 || applied.Sequence != latest.Sequence {
		t.Fatalf("recovered bus command: got setpoint %v sequence %d, want 4 at sequence %d",
			applied.Setpoint, applied.Sequence, latest.Sequence)
	}
}

// After a restart the emergency generation floor is re-armed from the
// snapshot, so pre-emergency commands stay rejected.
func TestRecoverLineReArmsEmergencyFloor(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	lineID := model.LineID("line-1")

	plant := newTestPlant(t, dir)
	batch := startTestBatch(t, plant, lineID)
	command, err := plant.ChangeDose(ctx, lineID, batch.ID, "pac", 4)
	if err != nil {
		t.Fatalf("change dose: %v", err)
	}
	if _, err := plant.EmergencyStop(ctx, lineID, "chlorine leak"); err != nil {
		t.Fatalf("emergency stop: %v", err)
	}

	recovered := newTestPlant(t, dir)
	if _, err := recovered.RecoverLine(ctx, lineID); err != nil {
		t.Fatalf("recover line: %v", err)
	}
	if err := recovered.devices.Publish(ctx, command); !errors.Is(err, ErrStaleCommand) {
		t.Fatalf("pre-emergency command after recovery: expected ErrStaleCommand, got %v", err)
	}
}

// A device command event that is already superseded must not abort recovery.
func TestApplyRecoveredToleratesStaleDeviceCommand(t *testing.T) {
	plant := newTestPlant(t, t.TempDir())
	ctx := context.Background()
	lineID := model.LineID("line-1")

	newer := testCommand("doser-line-1-pac", 1, 9, 4)
	if err := plant.devices.Publish(ctx, newer); err != nil {
		t.Fatalf("publish newer command: %v", err)
	}
	stale := testCommand("doser-line-1-pac", 1, 3, 2)
	event, err := model.NewEvent(lineID, 1, model.EventDeviceCommanded, stale, time.Now().UTC())
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	event.Sequence = 10
	if err := plant.ApplyRecovered(ctx, event); err != nil {
		t.Fatalf("stale command aborted recovery: %v", err)
	}
	if got := busCommand(t, plant.devices, "doser-line-1-pac").Setpoint; got != 4 {
		t.Fatalf("stale replayed command overrode bus: got %v, want 4", got)
	}
}
