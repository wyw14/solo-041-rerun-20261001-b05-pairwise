package dosing

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type capturePublisher struct {
	mu       sync.Mutex
	commands []model.DeviceCommand
}

func (c *capturePublisher) Publish(_ context.Context, command model.DeviceCommand) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = append(c.commands, command)
	return nil
}

func (c *capturePublisher) published() []model.DeviceCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]model.DeviceCommand(nil), c.commands...)
}

func newTestService(t *testing.T) (*Service, *capturePublisher) {
	t.Helper()
	store, err := journal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new journal: %v", err)
	}
	publisher := &capturePublisher{}
	service, err := NewService(store, publisher, NewLedger())
	if err != nil {
		t.Fatalf("new dosing service: %v", err)
	}
	return service, publisher
}

func testChange(t *testing.T, batchID uuid.UUID, rate float64) Change {
	t.Helper()
	change, err := NewChange("line-1", batchID, "pac", rate, 1, time.Now().UTC())
	if err != nil {
		t.Fatalf("new change: %v", err)
	}
	return change
}

// Commands carry the journal sequence of their authorizing event, so the
// device bus can order them inside one generation.
func TestChangeRateStampsJournalSequence(t *testing.T) {
	service, publisher := newTestService(t)
	ctx := context.Background()
	batchID := uuid.New()

	first, err := service.ChangeRate(ctx, testChange(t, batchID, 2), time.Now().UTC())
	if err != nil {
		t.Fatalf("change rate to 2: %v", err)
	}
	second, err := service.ChangeRate(ctx, testChange(t, batchID, 4), time.Now().UTC())
	if err != nil {
		t.Fatalf("change rate to 4: %v", err)
	}
	if first.Sequence == 0 || second.Sequence <= first.Sequence {
		t.Fatalf("commands not stamped with increasing journal sequences: %d, %d", first.Sequence, second.Sequence)
	}
	if first.DeviceID != "doser-line-1-pac" {
		t.Fatalf("device id is not line scoped: %s", first.DeviceID)
	}
	published := publisher.published()
	if len(published) != 2 || published[0].ID != first.ID || published[1].ID != second.ID {
		t.Fatalf("commands not published in journal order: %+v", published)
	}
	latest, ok := service.Latest(batchID)
	if !ok || latest.Rate != 4 {
		t.Fatalf("latest dose: got %+v, want rate 4", latest)
	}
}

// Concurrent changes are serialized: publish order matches journal order and
// the ledger ends on the same change the bus last received.
func TestChangeRateConcurrentSerialization(t *testing.T) {
	service, publisher := newTestService(t)
	ctx := context.Background()
	batchID := uuid.New()

	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		rate := float64(2 + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := service.ChangeRate(ctx, testChange(t, batchID, rate), time.Now().UTC()); err != nil {
				t.Errorf("change rate %v: %v", rate, err)
			}
		}()
	}
	wg.Wait()

	published := publisher.published()
	if len(published) != workers {
		t.Fatalf("published %d commands, want %d", len(published), workers)
	}
	for i := 1; i < len(published); i++ {
		if published[i].Sequence <= published[i-1].Sequence {
			t.Fatalf("publish order diverged from journal order at %d: %d after %d",
				i, published[i].Sequence, published[i-1].Sequence)
		}
	}
	latest, ok := service.Latest(batchID)
	if !ok {
		t.Fatal("ledger has no latest change")
	}
	last := published[len(published)-1]
	if latest.ID.String() != last.Metadata["change_id"] || latest.Rate != last.Setpoint {
		t.Fatalf("ledger latest %+v diverges from last published command %+v", latest, last)
	}
}
