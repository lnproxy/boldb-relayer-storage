package bolt

import (
	"context"
	"encoding/hex"
	"math/rand"

	"github.com/fiatjaf/relayer/v2"
	"github.com/nbd-wtf/go-nostr"
)

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func setupStorage(ss []relayer.Storage, n int) (ids, pubkeys []string, tags [][]string) {
	ctx := context.Background()
	pubkeys = make([]string, n/100+1)
	for i := range pubkeys {
		pubkeys[i] = randHex(32)
	}
	tags = make([][]string, n/100+1)
	for i := range tags {
		tags[i] = []string{"p", pubkeys[rand.Intn(len(pubkeys))]}
	}
	ids = make([]string, n)
	for i := 0; i < n; i++ {
		e := nostr.Event{
			ID:        randHex(32),
			PubKey:    pubkeys[rand.Intn(len(pubkeys))],
			CreatedAt: nostr.Timestamp(int64(i)),
			Kind:      rand.Intn(10),
			Tags:      nostr.Tags{tags[rand.Intn(len(tags))]},
			Content:   "arbitrary string",
			Sig:       randHex(64),
		}
		//e.Sign(sk)
		ids[i] = e.ID
		for _, s := range ss {
			s.SaveEvent(ctx, &e)
		}
	}
	return
}
