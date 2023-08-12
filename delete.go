package bolt

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"errors"

	"github.com/nbd-wtf/go-nostr"
	bolt "go.etcd.io/bbolt"
)

func (b *BoltBackend) DeleteEvent(ctx context.Context, id string, pubkey string) error {
	idb, _ := hex.DecodeString(id)
	pubkeyb, _ := hex.DecodeString(pubkey)

	return b.DB.Batch(func(tx *bolt.Tx) error {
		timestamps := tx.Bucket([]byte("timestamps"))
		timestamp_id := timestamps.Get(idb)
		if timestamp_id == nil {
			return errors.New("no timestamp")
		}
		timestamp_id = append(timestamp_id, idb...)

		authors := tx.Bucket([]byte("authors"))
		author := authors.Bucket(pubkeyb)
		if author == nil || author.Get(timestamp_id) == nil {
			return nil
		}

		events := tx.Bucket([]byte("events"))
		v := events.Get(idb)
		e := nostr.Event{}
		gob.NewDecoder(bytes.NewBuffer(v)).Decode(&e)
		idx := makeEventIndexBytes(&e)

		if err := events.Delete(idx.ID); err != nil {
			return err
		}

		if err := timestamps.Delete(idx.ID); err != nil {
			return err
		}

		timestamp_ids := tx.Bucket([]byte("timestamp_ids"))
		if err := timestamp_ids.Delete(idx.TimestampID); err != nil {
			return err
		}

		if err := author.Delete(idx.TimestampID); err != nil {
			return err
		}
		if k, _ := author.Cursor().First(); k == nil {
			if err := authors.DeleteBucket(idx.PubKey); err != nil {
				return err
			}
		}

		kinds := tx.Bucket([]byte("kinds"))
		kind := kinds.Bucket(idx.Kind)
		if err := kind.Delete(idx.TimestampID); err != nil {
			return err
		}
		if k, _ := kind.Cursor().First(); k == nil {
			if err := kinds.DeleteBucket(idx.Kind); err != nil {
				return err
			}
		}

		for tagKey, tagValues := range idx.Tags {
			if len(tagValues) == 0 {
				continue
			}
			tagBucket := tx.Bucket([]byte(tagKey))
			for _, tagValue := range tagValues {
				tagSubBucket := tagBucket.Bucket(tagValue)
				if err := tagSubBucket.Delete(idx.TimestampID); err != nil {
					return err
				}
				if k, _ := tagSubBucket.Cursor().First(); k == nil {
					if err := tagBucket.DeleteBucket(tagValue); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}
