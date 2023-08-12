package bolt

import (
	"time"

	bolt "go.etcd.io/bbolt"
)

func (b *BoltBackend) Init() error {
	db, err := bolt.Open(b.DatabaseURL, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	b.DB = db
	//b.DB.FreelistType = "hashmap"
	//b.DB.MaxBatchDelay = time.Second

	err = b.DB.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("events")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("authors")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("kinds")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("timestamps")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("timestamp_ids")); err != nil {
			return err
		}
		return nil
	})

	return err
}
