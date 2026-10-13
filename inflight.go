package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Inflight tracks requests that are currently being processed (waiting on the upstream, backing off
// between retries, or streaming a response) so the dashboard can show them live.
type Inflight struct {
	mu   sync.Mutex
	seq  int64
	reqs map[int64]*liveReq
}

type liveReq struct {
	id                      int64
	start                   time.Time
	session, source         string
	client, upstream        int64
	method, path, model     string
	stream                  bool
	msgCount                int
	attempts                int
	trail                   []string // outcome of each finished attempt: "429", "net error", ...
	state                   string
	bytes                   int64
	retryAt                 time.Time
	requestedModel, upModel string
}

func NewInflight() *Inflight { return &Inflight{reqs: map[int64]*liveReq{}} }

func (f *Inflight) add(l *liveReq) {
	f.mu.Lock()
	f.seq++
	l.id = f.seq
	f.reqs[l.id] = l
	f.mu.Unlock()
}

func (f *Inflight) remove(l *liveReq) {
	f.mu.Lock()
	delete(f.reqs, l.id)
	f.mu.Unlock()
}

func (f *Inflight) update(l *liveReq, fn func(*liveReq)) {
	f.mu.Lock()
	fn(l)
	f.mu.Unlock()
}

// Snapshot returns the active requests, oldest first, in the shape the dashboard expects.
func (f *Inflight) Snapshot() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	out := make([]map[string]any, 0, len(f.reqs))
	for _, l := range f.reqs {
		m := map[string]any{
			"id": l.id, "ts": l.start.Unix(), "elapsed_ms": now.Sub(l.start).Milliseconds(), "session_id": l.session,
			"session_source": l.source, "client_key_id": l.client, "upstream_key_id": l.upstream, "method": l.method,
			"path": l.path, "model": l.model, "requested_model": l.requestedModel, "stream": l.stream, "msg_count": l.msgCount,
			"attempts": l.attempts, "attempt_log": strings.Join(l.trail, " → "), "state": l.state, "bytes": l.bytes,
		}
		if !l.retryAt.IsZero() && l.retryAt.After(now) {
			m["retry_in_ms"] = l.retryAt.Sub(now).Milliseconds()
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["id"].(int64) < out[j]["id"].(int64) })
	return out
}

func statusLabel(status int, err error) string {
	if err != nil {
		return "net error"
	}
	return strconv.Itoa(status)
}
