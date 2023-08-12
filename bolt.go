package bolt

import (
	bolt "go.etcd.io/bbolt"
)

type BoltBackend struct {
	*bolt.DB
	DatabaseURL string
}
