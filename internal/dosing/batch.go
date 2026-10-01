package dosing

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type Change struct {
	ID          uuid.UUID    `json:"id"`
	LineID      model.LineID `json:"line_id"`
	BatchID     uuid.UUID    `json:"batch_id"`
	Chemical    string       `json:"chemical"`
	Rate        float64      `json:"rate"`
	Generation  uint64       `json:"generation"`
	RequestedAt time.Time    `json:"requested_at"`
}

func NewChange(lineID model.LineID, batchID uuid.UUID, chemical string, rate float64, generation uint64, now time.Time) (Change, error) {
	if lineID == "" || batchID == uuid.Nil || chemical == "" || rate <= 0 || generation == 0 {
		return Change{}, errors.New("dosing change is incomplete")
	}
	return Change{
		ID: uuid.New(), LineID: lineID, BatchID: batchID, Chemical: chemical,
		Rate: rate, Generation: generation, RequestedAt: now.UTC(),
	}, nil
}

type Ledger struct {
	mu      sync.RWMutex
	changes map[uuid.UUID][]Change
}

func NewLedger() *Ledger {
	return &Ledger{changes: make(map[uuid.UUID][]Change)}
}

func (l *Ledger) Record(change Change) {
	l.mu.Lock()
	defer l.mu.Unlock()
	values := l.changes[change.BatchID]
	if len(values) > 0 && values[len(values)-1].ID == change.ID {
		// A retried record of the latest change must not be appended twice.
		return
	}
	l.changes[change.BatchID] = append(values, change)
}

func (l *Ledger) Latest(batchID uuid.UUID) (Change, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	values := l.changes[batchID]
	if len(values) == 0 {
		return Change{}, false
	}
	return values[len(values)-1], true
}
