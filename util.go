package bolt

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/nbd-wtf/go-nostr"
)

type eventIndexBytes struct {
	ID          []byte
	PubKey      []byte
	Timestamp   []byte
	TimestampID []byte
	Kind        []byte
	Tags        map[string][][]byte
}

func makeEventIndexBytes(evt *nostr.Event) *eventIndexBytes {
	r := eventIndexBytes{}
	r.ID, _ = hex.DecodeString(evt.ID)
	r.PubKey, _ = hex.DecodeString(evt.PubKey)
	r.TimestampID = make([]byte, 8+32)
	binary.BigEndian.PutUint64(r.TimestampID, uint64(evt.CreatedAt))
	copy(r.TimestampID[8:], r.ID)
	r.Kind = make([]byte, 8)
	binary.BigEndian.PutUint64(r.Kind, uint64(evt.Kind))
	r.Tags = make(map[string][][]byte, 2)
	for _, tag := range evt.Tags {
		if (len(tag)) > 1 && len(tag[0]) == 1 && len(tag[1]) <= 200 {
			r.Tags[tag[0]] = append(r.Tags[tag[0]], []byte(tag[1]))
		}
	}
	return &r
}

type filterIndexBytes struct {
	IDs     [][]byte
	Kinds   [][]byte
	Authors [][]byte
	Tags    map[string][][]byte
	Since   []byte
	Until   []byte
}

func makeFilterIndexBytes(filter *nostr.Filter) *filterIndexBytes {
	r := filterIndexBytes{}
	if filter.Since != nil {
		r.Since = make([]byte, 8)
		binary.BigEndian.PutUint64(r.Since, uint64(*filter.Since))
	}
	if filter.Until != nil {
		r.Until = []byte{255, 255, 255, 255, 255, 255, 255, 255}
		binary.BigEndian.PutUint64(r.Until, uint64(*filter.Until)-1)
	}
	r.IDs = make([][]byte, len(filter.IDs))
	for i, id := range filter.IDs {
		idb, _ := hex.DecodeString(id)
		r.IDs[i] = idb
	}
	r.Authors = make([][]byte, len(filter.Authors))
	for i, author := range filter.Authors {
		authorb, _ := hex.DecodeString(author)
		r.Authors[i] = authorb
	}
	r.Kinds = make([][]byte, len(filter.Kinds))
	for i, kind := range filter.Kinds {
		kindb := make([]byte, 8)
		binary.BigEndian.PutUint64(kindb, uint64(kind))
		r.Kinds[i] = kindb
	}
	r.Tags = make(map[string][][]byte, 2)
	for k, vs := range filter.Tags {
		if len(k) != 1 {
			continue
		}
		for _, v := range vs {
			if len(v) <= 200 {
				r.Tags[k] = append(r.Tags[k], []byte(v))
			}
		}
	}
	return &r
}
