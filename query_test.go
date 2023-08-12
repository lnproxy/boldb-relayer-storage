package bolt

import (
	"context"
	"math/rand"
	"os"
	"testing"

	"github.com/fiatjaf/relayer/v2"
	"github.com/fiatjaf/relayer/v2/storage/sqlite3"
	"github.com/nbd-wtf/go-nostr"
)

func TestQueryEvents(t *testing.T) {
	f, _ := os.CreateTemp("", "")
	f.Close()
	defer os.Remove(f.Name())
	s := &BoltBackend{DatabaseURL: f.Name()}
	s.Init()
	// Disable batching since no parallel writes in benchmarks
	s.DB.MaxBatchSize = 0

	n := 10_000
	ids, pubkeys, tags := setupStorage([]relayer.Storage{s}, n)

	ctx := context.Background()
	limit := 5

	t.Run("IDs", func(t *testing.T) {
		filter := &nostr.Filter{IDs: []string{
			ids[rand.Intn(len(ids))],
			ids[rand.Intn(len(ids))],
			ids[rand.Intn(len(ids))],
			ids[rand.Intn(len(ids))],
			ids[rand.Intn(len(ids))],
		}}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Authors", func(t *testing.T) {
		filter := &nostr.Filter{
			Authors: []string{
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
			},
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Tags", func(t *testing.T) {
		tagMap := make(map[string][]string, 1)
		tag := tags[rand.Intn(len(tags))]
		tagMap[tag[0]] = []string{tag[1]}
		filter := &nostr.Filter{
			Tags:  tagMap,
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Kinds", func(t *testing.T) {
		filter := &nostr.Filter{
			Kinds: []int{
				rand.Intn(10),
				rand.Intn(10),
				rand.Intn(10),
			},
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Authors,Kinds", func(t *testing.T) {
		filter := &nostr.Filter{
			Authors: []string{
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
			},
			Kinds: []int{
				rand.Intn(10),
				rand.Intn(10),
				rand.Intn(10),
			},
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Since", func(t *testing.T) {
		since := nostr.Timestamp(n / 5)
		filter := &nostr.Filter{
			Since: &since,
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Authors,Since", func(t *testing.T) {
		since := nostr.Timestamp(n / 5)
		filter := &nostr.Filter{
			Authors: []string{
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
			},
			Since: &since,
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Authors,Kinds,Since", func(t *testing.T) {
		since := nostr.Timestamp(n / 10)
		filter := &nostr.Filter{
			Authors: []string{
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
			},
			Kinds: []int{
				rand.Intn(10),
				rand.Intn(10),
				rand.Intn(10),
				rand.Intn(10),
				rand.Intn(10),
				rand.Intn(10),
			},
			Since: &since,
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Tags,Kinds", func(t *testing.T) {
		tagMap := make(map[string][]string, 1)
		tag := tags[rand.Intn(len(tags))]
		tagMap[tag[0]] = []string{tag[1]}
		filter := &nostr.Filter{
			Tags:  tagMap,
			Kinds: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
	t.Run("Tags,Authors", func(t *testing.T) {
		tagMap := make(map[string][]string, 1)
		tag := tags[rand.Intn(len(tags))]
		tagMap[tag[0]] = []string{tag[1]}
		filter := &nostr.Filter{
			Tags: tagMap,
			Authors: []string{
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
				pubkeys[rand.Intn(len(pubkeys))],
			},
			Limit: limit,
		}
		ch, _ := s.QueryEvents(ctx, filter)
		var i int
		for e := range ch {
			i++
			if !filter.Matches(e) {
				t.Error("filter mismatch", filter, e)
			}
		}
		if i != limit {
			t.Error("unexpected number of events", i)
		}
	})
}

func queryEvents(b *testing.B, s relayer.Storage, n int) {
	ctx := context.Background()
	limit := 50
	ids, pubkeys, tags := setupStorage([]relayer.Storage{s}, n)
	b.ResetTimer()
	b.Run("IDs", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			filter := &nostr.Filter{IDs: []string{
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
				ids[rand.Intn(len(ids))],
			}}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Authors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			filter := &nostr.Filter{
				Authors: []string{
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
				},
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Tags", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tagMap := make(map[string][]string, 1)
			tag := tags[rand.Intn(len(tags))]
			tagMap[tag[0]] = []string{tag[1]}
			filter := &nostr.Filter{
				Tags:  tagMap,
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Kinds", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			filter := &nostr.Filter{
				Kinds: []int{
					rand.Intn(10),
					rand.Intn(10),
					rand.Intn(10),
				},
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Authors,Kinds", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			filter := &nostr.Filter{
				Authors: []string{
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
				},
				Kinds: []int{
					rand.Intn(10),
					rand.Intn(10),
					rand.Intn(10),
				},
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Since", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			since := nostr.Timestamp(n / 5)
			filter := &nostr.Filter{
				Since: &since,
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Authors,Since", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			since := nostr.Timestamp(n / 5)
			filter := &nostr.Filter{
				Authors: []string{
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
				},
				Since: &since,
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Authors,Kinds,Since", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			since := nostr.Timestamp(n / 10)
			filter := &nostr.Filter{
				Authors: []string{
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
				},
				Kinds: []int{
					rand.Intn(10),
					rand.Intn(10),
					rand.Intn(10),
					rand.Intn(10),
					rand.Intn(10),
					rand.Intn(10),
				},
				Since: &since,
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Tags,Kinds", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tagMap := make(map[string][]string, 1)
			tag := tags[rand.Intn(len(tags))]
			tagMap[tag[0]] = []string{tag[1]}
			filter := &nostr.Filter{
				Tags:  tagMap,
				Kinds: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
	b.Run("Tags,Authors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tagMap := make(map[string][]string, 1)
			tag := tags[rand.Intn(len(tags))]
			tagMap[tag[0]] = []string{tag[1]}
			filter := &nostr.Filter{
				Tags: tagMap,
				Authors: []string{
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
					pubkeys[rand.Intn(len(pubkeys))],
				},
				Limit: limit,
			}
			ch, _ := s.QueryEvents(ctx, filter)
			for _ = range ch {
			}
		}
	})
}

func BenchmarkSQLite3QueryEvents(b *testing.B) {
	f, _ := os.CreateTemp("", "")
	f.Close()
	defer os.Remove(f.Name())
	s := &sqlite3.SQLite3Backend{DatabaseURL: f.Name()}
	s.Init()
	queryEvents(b, s, 25_000)
}

func BenchmarkBoltQueryEvents(b *testing.B) {
	f, _ := os.CreateTemp("", "")
	f.Close()
	defer os.Remove(f.Name())
	s := &BoltBackend{DatabaseURL: f.Name()}
	s.Init()
	// Disable batching since no parallel writes in tests
	s.DB.MaxBatchSize = 0
	queryEvents(b, s, 25_000)
}
