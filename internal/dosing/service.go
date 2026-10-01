package dosing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type CommandPublisher interface {
	Publish(context.Context, model.DeviceCommand) error
}

type Service struct {
	journal   journal.Appender
	publisher CommandPublisher
	ledger    *Ledger
	// mu serializes a rate change so journal order, publish order and ledger
	// order stay identical: a slower concurrent change can never overwrite a
	// newer one that already committed.
	mu sync.Mutex
}

func NewService(appender journal.Appender, publisher CommandPublisher, ledger *Ledger) (*Service, error) {
	if appender == nil || publisher == nil || ledger == nil {
		return nil, errors.New("dosing service dependencies are required")
	}
	return &Service{journal: appender, publisher: publisher, ledger: ledger}, nil
}

func (s *Service) ChangeRate(ctx context.Context, change Change, now time.Time) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event, err := model.NewEvent(change.LineID, change.Generation, model.EventDosingChanged, change, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	persisted, err := s.journal.Append(ctx, event)
	if err != nil {
		return model.DeviceCommand{}, fmt.Errorf("append dosing batch failed: %w", err)
	}
	command, err := commandFor(change, persisted.Sequence, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := s.publisher.Publish(ctx, command); err != nil {
		return model.DeviceCommand{}, fmt.Errorf("publish dosing command: %w", err)
	}
	s.ledger.Record(change)
	return command, nil
}

// RecoveredCommand rebuilds the command a journaled change produced and
// records the change in the ledger, so replaying the journal restores the
// same device ordering and the same latest dose as live operation.
func (s *Service) RecoveredCommand(change Change, sequence uint64, occurredAt time.Time) (model.DeviceCommand, error) {
	command, err := commandFor(change, sequence, occurredAt)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	s.ledger.Record(change)
	return command, nil
}

func (s *Service) Latest(batchID uuid.UUID) (Change, bool) {
	return s.ledger.Latest(batchID)
}

func commandFor(change Change, sequence uint64, now time.Time) (model.DeviceCommand, error) {
	deviceID := "doser-" + string(change.LineID) + "-" + change.Chemical
	command, err := model.NewDeviceCommand(change.LineID, change.BatchID, deviceID, model.DeviceDoser, change.Generation, "set", now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	command.Sequence = sequence
	command.Setpoint = change.Rate
	command.Metadata["change_id"] = change.ID.String()
	return command, nil
}
