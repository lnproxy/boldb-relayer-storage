package bolt

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/fiatjaf/relayer/v2"
	"github.com/fiatjaf/relayer/v2/storage/sqlite3"
	"github.com/nbd-wtf/go-nostr"
)

func TestSaveEvent(t *testing.T) {
	f, _ := os.CreateTemp("", "")
	f.Close()
	defer os.Remove(f.Name())
	s := &BoltBackend{DatabaseURL: f.Name()}
	s.Init()
	// Disable batching since no parallel writes in tests
	s.DB.MaxBatchSize = 0

	ctx := context.Background()
	e := nostr.Event{
		ID:        randHex(32),
		PubKey:    randHex(32),
		CreatedAt: nostr.Timestamp(rand.Int63()),
		Kind:      rand.Intn(10),
		Tags:      nostr.Tags{nostr.Tag{"p", randHex(32)}},
		Content:   "arbitrary string",
		Sig:       randHex(64),
	}

	if err := s.SaveEvent(ctx, &e); err != nil {
		t.Fail()
	}

	ch, err := s.QueryEvents(ctx, &nostr.Filter{IDs: []string{e.ID}})
	if err != nil {
		t.Fail()
	}

	select {
	case e2 := <-ch:
		if e.ID != e2.ID {
			t.Fail()
		}
	case <-time.After(time.Second * 2):
		t.Fail()
	}
}

func saveEvents(b *testing.B, s relayer.Storage) {
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := nostr.Event{
			ID:        randHex(32),
			PubKey:    randHex(32),
			CreatedAt: nostr.Timestamp(rand.Int63()),
			Kind:      rand.Intn(10),
			Tags:      nostr.Tags{nostr.Tag{"p", randHex(32)}},
			Content:   "arbitrary string",
			Sig:       randHex(64),
		}
		//e.Sign(sk)
		s.SaveEvent(ctx, &e)
	}
}

func BenchmarkSaveEvent(b *testing.B) {
	f1, _ := os.CreateTemp("", "")
	f1.Close()
	defer os.Remove(f1.Name())
	sql := &sqlite3.SQLite3Backend{DatabaseURL: f1.Name()}
	sql.Init()

	f2, _ := os.CreateTemp("", "")
	f2.Close()
	defer os.Remove(f2.Name())
	nosql := &BoltBackend{DatabaseURL: f2.Name()}
	nosql.Init()
	// Disable batching since no parallel writes in benchmarks
	nosql.DB.MaxBatchSize = 0

	setupStorage([]relayer.Storage{sql, nosql}, 10_000)

	b.Run("SQLite3Backend", func(b *testing.B) { saveEvents(b, sql) })
	b.Run("BoltBackend", func(b *testing.B) { saveEvents(b, nosql) })
}
