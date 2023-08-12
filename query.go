package bolt

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"log"
	"time"

	"github.com/nbd-wtf/go-nostr"
	bolt "go.etcd.io/bbolt"
)

func (b BoltBackend) QueryEvents(ctx context.Context, filter *nostr.Filter) (ch chan *nostr.Event, err error) {
	full_ids, err := checkFilter(filter)
	if err != nil {
		log.Println("rejected query:", err, filter)
		return nil, nil
	}
	idx := makeFilterIndexBytes(filter)
	ch = make(chan *nostr.Event)
	go b.DB.View(func(tx *bolt.Tx) error {
		ctx, cancel := context.WithCancel(context.Background())
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
					log.Printf("query for %v has been running for %s", filter, now.Sub(start))
				}
			}
		}()
		defer close(ch)
		limit := filter.Limit
		events := tx.Bucket([]byte("events"))

		switch {
		// No filter:
		case filter.IDs == nil && filter.Kinds == nil && filter.Authors == nil && len(filter.Tags) == 0:
			c := tx.Bucket([]byte("timestamp_ids")).Cursor()
			var k, v []byte
			if filter.Until != nil {
				k, v = c.Seek(idx.Until)
			} else {
				k, v = c.Last()
			}
			for ; k != nil && limit > 0 && bytes.Compare(k, idx.Since) >= 0; k, v = c.Prev() {
				limit -= 1
				evt := nostr.Event{}
				v = events.Get(k[8:len(k)])
				gob.NewDecoder(bytes.NewBuffer(v)).Decode(&evt)
				ch <- &evt
			}

		// ID Filters (no prefix filters):
		case filter.IDs != nil && full_ids && filter.Kinds == nil && filter.Authors == nil && len(filter.Tags) == 0:
			for _, id := range idx.IDs {
				v := events.Get(id)
				if v != nil {
					evt := nostr.Event{}
					gob.NewDecoder(bytes.NewBuffer(v)).Decode(&evt)
					ch <- &evt
				}
			}

		// ID Filters (allow prefix filters):
		case filter.IDs != nil && filter.Kinds == nil && filter.Authors == nil && len(filter.Tags) == 0:
			c := events.Cursor()
			for _, prefix := range idx.IDs {
				for k, v := c.Seek(prefix); limit > 0 && bytes.HasPrefix(k, prefix); k, v = c.Next() {
					limit -= 1
					evt := nostr.Event{}
					gob.NewDecoder(bytes.NewBuffer(v)).Decode(&evt)
					ch <- &evt
				}
			}

		// Non-id Filters:
		case filter.IDs == nil:
			andCursorSlice := make([]CursorLike, 0, 3)

			if filter.Authors != nil && len(filter.Authors) > 0 {
				b := tx.Bucket([]byte("authors"))
				cs := make([]CursorLike, 0, len(filter.Authors))
				for _, author := range idx.Authors {
					sb := b.Bucket(author)
					if sb != nil {
						cs = append(cs, sb.Cursor())
					}
				}
				if len(cs) == 0 { //no events match
					return nil
				}
				andCursorSlice = append(andCursorSlice, makeOrCursor(cs))
			}

			for tagKey, tagValues := range idx.Tags {
				if len(tagValues) == 0 {
					continue
				}
				tagBucket := tx.Bucket([]byte(tagKey))
				if tagBucket == nil {
					return nil
				}
				cs := make([]CursorLike, 0, len(tagValues))
				for _, tagValue := range tagValues {
					tagSubBucket := tagBucket.Bucket(tagValue)
					if tagSubBucket != nil {
						cs = append(cs, tagSubBucket.Cursor())
					}
				}
				if len(cs) == 0 { //no events match
					return nil
				}
				andCursorSlice = append(andCursorSlice, makeOrCursor(cs))
			}

			if filter.Kinds != nil && len(filter.Kinds) > 0 {
				b := tx.Bucket([]byte("kinds"))
				cs := make([]CursorLike, 0, len(filter.Kinds))
				for _, kind := range idx.Kinds {
					sb := b.Bucket(kind)
					if sb != nil {
						cs = append(cs, sb.Cursor())
					}
				}
				if len(cs) == 0 { //no events match
					return nil
				}
				andCursorSlice = append(andCursorSlice, makeOrCursor(cs))
			}

			if len(andCursorSlice) == 0 {
				return nil
			}
			c := makeAndCursor(andCursorSlice)

			var k, v []byte
			if filter.Until != nil {
				k, _ = c.Seek(idx.Until)
			} else {
				k, _ = c.Last()
			}
			for ; k != nil && limit > 0 && bytes.Compare(k, idx.Since) >= 0; k, _ = c.Prev() {
				limit -= 1
				evt := nostr.Event{}
				v = events.Get(k[8:len(k)])
				gob.NewDecoder(bytes.NewBuffer(v)).Decode(&evt)
				ch <- &evt
			}
		}
		return nil
	})

	return ch, nil
}

func checkFilter(filter *nostr.Filter) (full_ids bool, err error) {
	if filter == nil {
		return false, errors.New("filter cannot be null")
	}
	if filter.Search != "" {
		return false, errors.New("search filter not supported")
	}
	if filter.Limit < 1 || filter.Limit > 100 {
		filter.Limit = 100
	}
	if filter.IDs != nil {
		if len(filter.IDs) == 0 || len(filter.IDs) > 100 {
			return false, errors.New("filter has invalid number of ids")
		}
		full_ids = true
		for _, author := range filter.Authors {
			if len(author) != 64 {
				full_ids = false
			}
		}
	}
	if filter.Authors != nil {
		if len(filter.Authors) == 0 || len(filter.Authors) > 100 {
			return false, errors.New("filter has invalid number of authors")
		}
		for _, author := range filter.Authors {
			if len(author) != 64 {
				return false, errors.New("authors prefix filter not supported")
			}
		}
	}
	if filter.Kinds != nil {
		if len(filter.Kinds) == 0 || len(filter.Kinds) > 10 {
			return false, errors.New("filter has invalid number of Kinds")
		}
	}
	if len(filter.Tags) > 100 {
		return false, errors.New("filter has invalid number of Tags")
	}
	for k, v := range filter.Tags {
		if len(k) != 1 {
			return false, errors.New("filters on this tag not supported")
		}
		if len(v) > 100 {
			return false, errors.New("tag has invalid number of values")
		}
	}

	return
}

type CursorLike interface {
	Last() ([]byte, []byte)
	Prev() ([]byte, []byte)
	Seek(seek []byte) ([]byte, []byte)
}

type orCursor struct {
	cursors []CursorLike
	keys    [][]byte
	values  [][]byte
	emited  map[string]struct{}
}

func makeOrCursor(cs []CursorLike) CursorLike {
	if len(cs) == 0 {
		panic("empty argument to makeOrCursor")
	}
	if len(cs) == 1 {
		return cs[0]
	}
	return &orCursor{
		cursors: cs,
		keys:    make([][]byte, len(cs)),
		emited:  make(map[string]struct{}, len(cs)),
	}
}

func (oc *orCursor) Last() (key, value []byte) {
	for k := range oc.emited {
		delete(oc.emited, k)
	}
	var max []byte
	for i, c := range oc.cursors {
		k, _ := c.Last()
		oc.keys[i] = k
		if bytes.Compare(k, max) > 0 {
			max = k
		}
	}
	oc.emited[string(max)] = struct{}{}
	return max, nil
}

func (oc *orCursor) Prev() (key, value []byte) {
	var max []byte
	for i, c := range oc.cursors {
		for k := oc.keys[i]; k != nil; k, _ = c.Prev() {
			if _, ok := oc.emited[string(k)]; ok {
				continue
			}
			oc.keys[i] = k
			if bytes.Compare(k, max) > 0 {
				max = k
			}
			break
		}
	}
	oc.emited[string(max)] = struct{}{}
	return max, nil
}

func (oc *orCursor) Seek(seek []byte) (key, value []byte) {
	for k := range oc.emited {
		delete(oc.emited, k)
	}
	var max []byte
	for i, c := range oc.cursors {
		k, _ := c.Seek(seek)
		oc.keys[i] = k
		if bytes.Compare(k, max) > 0 {
			max = k
		}
	}
	oc.emited[string(max)] = struct{}{}
	return max, nil
}

type andCursor struct {
	cursors []CursorLike
	keys    [][]byte
}

func makeAndCursor(cs []CursorLike) CursorLike {
	if len(cs) == 0 {
		panic("empty argument to makeAndCursor")
	}
	if len(cs) == 1 {
		return cs[0]
	}
	return &andCursor{
		cursors: cs,
		keys:    make([][]byte, len(cs)),
	}
}

func (ac *andCursor) Last() (key, value []byte) {
	for i, c := range ac.cursors {
		ac.keys[i], _ = c.Last()
	}
	return ac.Prev()
}

func minByteSlice(xs [][]byte) []byte {
	min := xs[0]
	for _, x := range xs {
		if bytes.Compare(x, min) < 0 {
			min = x
		}
	}
	return min
}

func (ac *andCursor) Prev() (key, value []byte) {
	var min []byte
	var allMin bool
	for {
		min = minByteSlice(ac.keys)
		if min == nil {
			break
		}
		allMin = true
		for i, k := range ac.keys {
			c := ac.cursors[i]
			for k, _ = c.Seek(min); k != nil && bytes.Compare(k, min) > 0; k, _ = c.Prev() {
			}
			ac.keys[i] = k
			if bytes.Compare(k, min) != 0 {
				allMin = false
			} else {
				ac.keys[i], _ = c.Prev()
			}
		}
		if allMin {
			break
		}
	}
	return min, nil
}

func (ac *andCursor) Seek(seek []byte) (key, value []byte) {
	for i, c := range ac.cursors {
		ac.keys[i], _ = c.Seek(seek)
	}
	return ac.Prev()
}
