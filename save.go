package bolt

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"time"

	"github.com/nbd-wtf/go-nostr"
	bolt "go.etcd.io/bbolt"
)

func (b *BoltBackend) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		duration := time.Second
		timer := time.NewTimer(duration)
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return // returning not to leak the goroutine
			case now := <-timer.C:
				duration *= 2
				timer.Reset(duration)
				log.Printf("SaveEvent for %v has been running for %s", evt, now.Sub(start))
			}
		}
	}()
	idx := makeEventIndexBytes(evt)
	alreadySaved := false
	b.DB.View(func(tx *bolt.Tx) error {
		events := tx.Bucket([]byte("events"))
		if v := events.Get(idx.ID); v != nil {
			alreadySaved = true
		}
		return nil
	})
	if alreadySaved {
		return nil
	}
	var evtBuffer bytes.Buffer
	gob.NewEncoder(&evtBuffer).Encode(evt)
	evtBytes := evtBuffer.Bytes()
	return b.DB.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket([]byte("events"))
		if err := events.Put(idx.ID, evtBytes); err != nil {
			return err
		}
		authors := tx.Bucket([]byte("authors"))
		author, err := authors.CreateBucketIfNotExists(idx.PubKey)
		if err != nil {
			return err
		}
		if err := author.Put(idx.TimestampID, nil); err != nil {
			return err
		}
		kinds := tx.Bucket([]byte("kinds"))
		kind, err := kinds.CreateBucketIfNotExists(idx.Kind)
		if err != nil {
			return err
		}
		if err := kind.Put(idx.TimestampID, nil); err != nil {
			return err
		}
		timestamps := tx.Bucket([]byte("timestamps"))
		if err := timestamps.Put(idx.ID, idx.Timestamp); err != nil {
			return err
		}
		timestamp_ids := tx.Bucket([]byte("timestamp_ids"))
		if err := timestamp_ids.Put(idx.TimestampID, nil); err != nil {
			return err
		}
		for tagKey, tagValues := range idx.Tags {
			tagBucket, err := tx.CreateBucketIfNotExists([]byte(tagKey))
			if err != nil {
				return err
			}
			for _, tagValue := range tagValues {
				tagSubBucket, err := tagBucket.CreateBucketIfNotExists(tagValue)
				if err != nil {
					return err
				}
				if err := tagSubBucket.Put(idx.TimestampID, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (b *BoltBackend) BeforeSave(ctx context.Context, evt *nostr.Event) {
	// do nothing
}

func (b *BoltBackend) AfterSave(evt *nostr.Event) {
	// do nothing
}
