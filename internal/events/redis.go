// Package events keeps a bounded, shared SSE replay journal in Redis.
package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pos-term/payment-processor/internal/storage"
	"github.com/redis/go-redis/v9"
)

const (
	streamKey = "pos:transaction-events:v1"
	retention = 10000
	batchSize = 100
)

type Event struct {
	ID          string
	Kind        string
	Transaction storage.TransactionView
}

type Batch struct {
	Cursor string
	Resync bool
	Events []Event
}

type Store interface {
	GetTransaction(context.Context, string) (storage.TransactionView, bool, error)
}

// Redis has no consumer group: every HTTP connection sees the same events.
// Epochs distinguish a lost journal from a cursor belonging to the new journal.
type Redis struct {
	client *redis.Client
	key    string
	store  Store
	mu     sync.Mutex
	dirty  bool
}

func New(client *redis.Client, store Store, key string) *Redis {
	if key == "" {
		key = streamKey
	}
	return &Redis{client: client, store: store, key: key, dirty: true}
}

var appendScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 or redis.call('EXISTS', KEYS[2]) == 0 then
 redis.call('SET', KEYS[2], ARGV[1])
end
if ARGV[2] == '1' and ARGV[5] ~= 'resync' then
 redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[3], '*', 'kind', 'resync', 'data', '{}')
end
return redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[3], '*', 'kind', ARGV[5], 'data', ARGV[4])
`)

// TransactionUpdated reads the stored snapshot, never the incoming Kafka payload.
// Failures invalidate the next successful publication; they do not stop payments.
func (r *Redis) TransactionUpdated(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	v, ok, err := r.store.GetTransaction(ctx, id)
	if err != nil || !ok {
		r.dirty = true
		if err != nil {
			return fmt.Errorf("read SSE snapshot %s: %w", id, err)
		}
		return fmt.Errorf("SSE snapshot %s not found", id)
	}
	data, err := json.Marshal(v)
	if err != nil {
		r.dirty = true
		return err
	}
	return r.append(ctx, "transaction.updated", string(data))
}

// append is called with mu held. A reset before the next snapshot makes a failed
// notification visible even if the original payment already finished successfully.
func (r *Redis) append(ctx context.Context, kind, data string) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		r.dirty = true
		return err
	}
	dirty := "0"
	if r.dirty {
		dirty = "1"
	}
	err := appendScript.Run(ctx, r.client, []string{r.key, r.key + ":epoch"}, hex.EncodeToString(nonce[:]), dirty, retention, data, kind).Err()
	r.dirty = err != nil
	return err
}

// Run retries history invalidation after startup or a failed notification, even
// when no further payments arrive. Redis availability never controls service life.
func (r *Redis) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		_ = r.flush(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Redis) flush(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.dirty {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return r.append(ctx, "resync", "{}")
}

// Invalidate requests a history reload after replaying an already stored transaction:
// a previous process may have crashed between its Postgres and Redis writes.
func (r *Redis) Invalidate() { r.mu.Lock(); r.dirty = true; r.mu.Unlock() }

var readScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 or redis.call('EXISTS', KEYS[2]) == 0 then
 redis.call('SET', KEYS[2], ARGV[4])
 redis.call('XADD', KEYS[1], '*', 'kind', 'resync', 'data', '{}')
end
local epoch = redis.call('GET', KEYS[2]) or ''
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local tail = '0-0'
if #last > 0 then tail = last[1][1] end
local cursor = epoch .. '/' .. tail
if ARGV[1] == '' then return {0, cursor, {}} end
if ARGV[1] ~= epoch then return {1, cursor, {}} end
local after = ARGV[2]
local found = redis.call('XRANGE', KEYS[1], after, after, 'COUNT', 1)
if #found == 0 then return {1,cursor,{}} end
local rows = redis.call('XRANGE', KEYS[1], '(' .. after, '+', 'COUNT', ARGV[3])
if #rows > 0 then cursor = epoch .. '/' .. rows[#rows][1] else cursor = epoch .. '/' .. after end
return {0,cursor,rows}
`)

// Read atomically checks retention and reads a batch, avoiding a trim between the two.
func (r *Redis) Read(ctx context.Context, cursor string) (Batch, error) {
	epoch, id, valid := splitCursor(cursor)
	if cursor != "" && !valid {
		epoch = "invalid"
		id = "0-0"
	}
	if cursor == "" {
		id = "0-0"
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Batch{}, err
	}
	raw, err := readScript.Run(ctx, r.client, []string{r.key, r.key + ":epoch"}, epoch, id, batchSize, hex.EncodeToString(nonce[:])).Slice()
	if err != nil {
		return Batch{}, fmt.Errorf("read SSE journal: %w", err)
	}
	b := Batch{Resync: raw[0].(int64) == 1, Cursor: raw[1].(string)}
	for _, item := range raw[2].([]any) {
		row := item.([]any)
		fields := row[1].([]any)
		e := Event{ID: strings.SplitN(b.Cursor, "/", 2)[0] + "/" + row[0].(string)}
		for i := 0; i < len(fields); i += 2 {
			switch fields[i].(string) {
			case "kind":
				e.Kind = fields[i+1].(string)
			case "data":
				if err := json.Unmarshal([]byte(fields[i+1].(string)), &e.Transaction); err != nil {
					return Batch{}, fmt.Errorf("decode SSE journal: %w", err)
				}
			}
		}
		b.Events = append(b.Events, e)
	}
	return b, nil
}

func splitCursor(s string) (string, string, bool) {
	epoch, id, ok := strings.Cut(s, "/")
	if !ok || (epoch != "" && len(epoch) != 32) {
		return "", "", false
	}
	if epoch != "" {
		if _, err := hex.DecodeString(epoch); err != nil {
			return "", "", false
		}
	}
	a, b, ok := strings.Cut(id, "-")
	if !ok {
		return "", "", false
	}
	for _, part := range []string{a, b} {
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil || strconv.FormatUint(n, 10) != part {
			return "", "", false
		}
	}
	return epoch, id, true
}
