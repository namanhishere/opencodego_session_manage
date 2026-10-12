package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Session sources, recorded per request so the dashboard shows how a conversation was identified.
const (
	SrcHeader = "header" // client supplied x-opencode-session
	SrcChain  = "chain"  // request messages extend a previously seen message list
	SrcFirst  = "first"  // matched on system prompt + first user message
	SrcNew    = "new"    // no match: a new session was created
)

type Session struct {
	ID            string
	ClientKeyID   int64
	UpstreamKeyID int64
	Source        string
	Expires       time.Time
}

// Fingerprints derived from one request body.
type Fingerprints struct {
	Chain []string // Chain[i] identifies messages[:i+1]
	First string   // system prompt + first user message ("" if not derivable)
	Count int
}

// canonical message fields that define identity; everything else (ids, usage, reasoning) is ignored
// because clients differ in what they echo back.
var identityFields = []string{"role", "content", "tool_calls", "tool_call_id", "name", "type", "call_id", "arguments", "output"}

func canonMessage(m any) []byte {
	obj, ok := m.(map[string]any)
	if !ok {
		b, _ := json.Marshal(m)
		return b
	}
	out := map[string]any{}
	for _, f := range identityFields {
		if v, ok := obj[f]; ok {
			out[f] = v
		}
	}
	b, _ := json.Marshal(out) // map keys are sorted -> stable
	return b
}

// ComputeFingerprints works on chat-completions "messages" and responses-API "input" arrays.
// scope separates clients so identical prompts from different client keys never share a session.
func ComputeFingerprints(scope int64, body map[string]any) Fingerprints {
	var msgs []any
	if m, ok := body["messages"].([]any); ok {
		msgs = m
	} else if in, ok := body["input"].([]any); ok {
		msgs = in
	} else if s, ok := body["input"].(string); ok {
		msgs = []any{map[string]any{"role": "user", "content": s}}
	}
	fp := Fingerprints{Count: len(msgs)}
	if len(msgs) == 0 {
		return fp
	}
	h := sha256.New()
	fmt.Fprintf(h, "scope:%d|", scope)
	prev := h.Sum(nil)
	for _, m := range msgs {
		hh := sha256.New()
		hh.Write(prev)
		hh.Write(canonMessage(m))
		prev = hh.Sum(nil)
		fp.Chain = append(fp.Chain, "c:"+hex.EncodeToString(prev))
	}

	// fallback: system/developer prompt (+ top-level instructions) and first user message
	fh := sha256.New()
	fmt.Fprintf(fh, "first:%d|", scope)
	if ins, ok := body["instructions"].(string); ok {
		fmt.Fprintf(fh, "ins:%s|", ins)
	}
	foundUser := false
	for _, m := range msgs {
		obj, _ := m.(map[string]any)
		role, _ := obj["role"].(string)
		if role == "system" || role == "developer" {
			fh.Write(canonMessage(m))
			continue
		}
		if role == "user" {
			fh.Write(canonMessage(m))
			foundUser = true
		}
		break
	}
	if foundUser {
		fp.First = "f:" + hex.EncodeToString(fh.Sum(nil))
	}
	return fp
}

type Resolver struct {
	st *Store
	mu sync.Mutex // serialises resolve so concurrent first requests share one session
}

func NewResolver(st *Store) *Resolver { return &Resolver{st: st} }

// Resolve finds or creates the session for a request.
// Order: explicit header > longest message-prefix match > system+first-user match > new.
func (r *Resolver) Resolve(clientKeyID int64, headerID string, fp Fingerprints) (*Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()

	var id, src string
	if headerID != "" {
		id, src = fmt.Sprintf("c%d:%s", clientKeyID, headerID), SrcHeader
	} else {
		// longest prefix wins; a client resending the identical list (retry) matches at len(chain)-1
		for i := len(fp.Chain) - 1; i >= 0; i-- {
			if sid, ok := r.st.LookupFingerprint(fp.Chain[i]); ok {
				id, src = sid, SrcChain
				break
			}
		}
		if id == "" && fp.First != "" {
			if sid, ok := r.st.LookupFingerprint(fp.First); ok {
				id, src = sid, SrcFirst
			}
		}
	}

	if id != "" {
		if row, err := r.load(id); err == nil && !row.expired(now) {
			row.Source = src
			return row.toSession(), nil
		}
	}
	if src != SrcHeader {
		id, src = newSessionID(clientKeyID), SrcNew
	}
	return r.create(id, clientKeyID, src, now)
}

func newSessionID(clientKeyID int64) string {
	b := make([]byte, 12)
	randRead(b)
	return fmt.Sprintf("c%d:s_%s", clientKeyID, hex.EncodeToString(b))
}

type sessionRow struct {
	Session
	expiredFlag bool
}

func (s *sessionRow) expired(now time.Time) bool { return s.expiredFlag || !s.Expires.After(now) }

func (r *Resolver) load(id string) (*sessionRow, error) {
	var s sessionRow
	var exp int64
	var expired int
	err := r.st.db.QueryRow(`SELECT id, client_key_id, upstream_key_id, expires_at, expired FROM sessions WHERE id=?`, id).
		Scan(&s.ID, &s.ClientKeyID, &s.UpstreamKeyID, &exp, &expired)
	if err != nil {
		return nil, err
	}
	s.Expires = time.Unix(exp, 0)
	s.expiredFlag = expired == 1
	return &s, nil
}

func (r *sessionRow) toSession() *Session { return &r.Session }

func (r *Resolver) create(id string, clientKeyID int64, src string, now time.Time) (*Session, error) {
	up, err := r.pickUpstream(clientKeyID)
	if err != nil {
		return nil, err
	}
	exp := now.Add(r.st.SessionTTL())
	// REPLACE handles a header-supplied ID whose previous incarnation had expired: it starts fresh.
	_, err = r.st.db.Exec(`INSERT INTO sessions(id,client_key_id,upstream_key_id,source,created_at,last_seen,expires_at,expired)
		VALUES(?,?,?,?,?,?,?,0)
		ON CONFLICT(id) DO UPDATE SET upstream_key_id=excluded.upstream_key_id, source=excluded.source,
			created_at=excluded.created_at, last_seen=excluded.last_seen, expires_at=excluded.expires_at, expired=0`,
		id, clientKeyID, up, src, now.Unix(), now.Unix(), exp.Unix())
	if err != nil {
		return nil, err
	}
	return &Session{ID: id, ClientKeyID: clientKeyID, UpstreamKeyID: up, Source: src, Expires: exp}, nil
}

// pickUpstream chooses the enabled upstream key with the fewest live sessions (new sessions spread out;
// existing sessions stay pinned so routing/prompt caching stays consistent).
func (r *Resolver) pickUpstream(_ int64) (int64, error) {
	var id int64
	err := r.st.db.QueryRow(`
		SELECT k.id FROM upstream_keys k
		WHERE k.enabled=1
		ORDER BY (SELECT COUNT(*) FROM sessions s WHERE s.upstream_key_id=k.id AND s.expired=0 AND s.expires_at>?) ASC, k.id ASC
		LIMIT 1`, time.Now().Unix()).Scan(&id)
	if err != nil {
		return 0, errNoUpstream
	}
	return id, nil
}

// Touch records a finished request on the session: slides expiry, registers fingerprints, adds usage.
func (r *Resolver) Touch(s *Session, fp Fingerprints, u Usage) {
	now := time.Now()
	exp := now.Add(r.st.SessionTTL())
	r.st.db.Exec(`UPDATE sessions SET last_seen=?, expires_at=?, requests=requests+1,
		prompt_tokens=prompt_tokens+?, completion_tokens=completion_tokens+?, cached_tokens=cached_tokens+? WHERE id=?`,
		now.Unix(), exp.Unix(), u.Prompt, u.Completion, u.Cached, s.ID)
	var hs []string
	if n := len(fp.Chain); n > 0 {
		hs = append(hs, fp.Chain[n-1]) // only the full list: the next turn extends exactly this
	}
	if fp.First != "" {
		hs = append(hs, fp.First)
	}
	if len(hs) > 0 {
		r.st.PutFingerprints(s.ID, hs, exp.Unix())
	}
}
