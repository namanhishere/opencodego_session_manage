package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeUpstream struct {
	mu   sync.Mutex
	seen []http.Header
	body []string
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.seen = append(f.seen, r.Header.Clone())
	f.body = append(f.body, string(b))
	f.mu.Unlock()
	if strings.Contains(string(b), `"stream":true`) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
		io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":64}}}\n\ndata: [DONE]\n\n")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":10,"completion_tokens":2}}`)
}

func setup(t *testing.T) (*httptest.Server, *fakeUpstream, *Store, string) {
	t.Helper()
	st, err := OpenStore(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	fu := &fakeUpstream{}
	up := httptest.NewServer(fu)
	t.Cleanup(up.Close)
	rs := NewResolver(st)
	px := httptest.NewServer(NewProxy(st, rs, up.URL))
	t.Cleanup(px.Close)
	st.db.Exec(`INSERT INTO upstream_keys(name,key,enabled,created_at) VALUES('a','KEY-A',1,0),('b','KEY-B',1,0)`)
	ck := "ocp_testkey"
	st.db.Exec(`INSERT INTO client_keys(name,hash,prefix,enabled,created_at) VALUES('c',?,?,1,0)`, hashKey(ck), ck[:8])
	return px, fu, st, ck
}

func call(t *testing.T, px *httptest.Server, key string, body map[string]any, hdr map[string]string) (string, string) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", px.URL+"/v1/chat/completions", strings.NewReader(string(b)))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	return resp.Header.Get("x-proxy-session"), ""
}

func msgs(parts ...string) []any {
	var out []any
	for i, p := range parts {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		out = append(out, map[string]any{"role": role, "content": p})
	}
	return out
}

func TestChainKeepsSessionAndKey(t *testing.T) {
	px, fu, _, ck := setup(t)
	s1, _ := call(t, px, ck, map[string]any{"model": "m", "messages": msgs("hello")}, nil)
	s2, _ := call(t, px, ck, map[string]any{"model": "m", "messages": msgs("hello", "hi!", "and?")}, nil)
	s3, _ := call(t, px, ck, map[string]any{"model": "m", "messages": msgs("hello", "hi!", "and?", "x", "more")}, nil)
	if s1 == "" || s1 != s2 || s2 != s3 {
		t.Fatalf("sessions differ: %q %q %q", s1, s2, s3)
	}
	// unrelated conversation -> new session, spread to the other upstream key
	s4, _ := call(t, px, ck, map[string]any{"model": "m", "messages": msgs("different")}, nil)
	if s4 == s1 {
		t.Fatal("unrelated conversation merged")
	}
	fu.mu.Lock()
	defer fu.mu.Unlock()
	if fu.seen[0].Get("Authorization") != fu.seen[2].Get("Authorization") {
		t.Fatal("session not sticky to one upstream key")
	}
	if fu.seen[0].Get("Authorization") == fu.seen[3].Get("Authorization") {
		t.Fatal("new session was not spread to the other key")
	}
	if fu.seen[0].Get("x-opencode-session") != s1 {
		t.Fatal("session header not forwarded upstream")
	}
}

func TestFirstMessageFallbackWhenHistoryRewritten(t *testing.T) {
	px, _, _, ck := setup(t)
	sys := map[string]any{"role": "system", "content": "you are x"}
	a, _ := call(t, px, ck, map[string]any{"messages": []any{sys, msgs("q1")[0]}}, nil)
	// client rewrote the middle (e.g. stripped reasoning) -> chain misses, first-user hash still matches
	b, _ := call(t, px, ck, map[string]any{"messages": []any{sys, msgs("q1")[0], map[string]any{"role": "assistant", "content": "DIFFERENT"}, map[string]any{"role": "user", "content": "q2"}}}, nil)
	if a != b {
		t.Fatalf("fallback failed: %q vs %q", a, b)
	}
}

func TestHeaderWinsAndIsScopedPerClient(t *testing.T) {
	px, _, _, ck := setup(t)
	a, _ := call(t, px, ck, map[string]any{"messages": msgs("one")}, map[string]string{"x-opencode-session": "abc"})
	b, _ := call(t, px, ck, map[string]any{"messages": msgs("completely other")}, map[string]string{"x-opencode-session": "abc"})
	if a != b || !strings.HasSuffix(a, ":abc") {
		t.Fatalf("header session: %q %q", a, b)
	}
}

func TestUsageAndStreamOptions(t *testing.T) {
	px, fu, st, ck := setup(t)
	call(t, px, ck, map[string]any{"messages": msgs("s"), "stream": true}, nil)
	var p, c, cached int
	st.db.QueryRow(`SELECT prompt_tokens, completion_tokens, cached_tokens FROM requests ORDER BY id DESC LIMIT 1`).Scan(&p, &c, &cached)
	if p != 100 || c != 7 || cached != 64 {
		t.Fatalf("usage %d %d %d", p, c, cached)
	}
	if !strings.Contains(fu.body[0], "include_usage") {
		t.Fatal("include_usage not injected")
	}
	call(t, px, ck, map[string]any{"messages": msgs("ns")}, nil)
	st.db.QueryRow(`SELECT prompt_tokens FROM requests ORDER BY id DESC LIMIT 1`).Scan(&p)
	if p != 10 {
		t.Fatalf("non-stream usage %d", p)
	}
}

func TestExpiredSessionStartsFresh(t *testing.T) {
	px, _, st, ck := setup(t)
	a, _ := call(t, px, ck, map[string]any{"messages": msgs("hello")}, nil)
	st.db.Exec(`UPDATE sessions SET expires_at=1, expired=0`)
	st.db.Exec(`UPDATE fingerprints SET expires_at=1`)
	st.mu.Lock()
	for h, e := range st.fp {
		e.expires = 1
		st.fp[h] = e
	}
	st.mu.Unlock()
	b, _ := call(t, px, ck, map[string]any{"messages": msgs("hello", "hi", "again")}, nil)
	if a == b {
		t.Fatal("expired session was reused")
	}
}

func TestAuthRejected(t *testing.T) {
	px, _, _, _ := setup(t)
	req, _ := http.NewRequest("POST", px.URL+"/v1/chat/completions", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer nope")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 401 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestStreamTraceAndFinishRecorded(t *testing.T) {
	px, _, st, ck := setup(t)
	b, _ := json.Marshal(map[string]any{"model": "m", "stream": true, "messages": msgs("hello")})
	req, _ := http.NewRequest("POST", px.URL+"/v1/chat/completions", strings.NewReader(string(b)))
	req.Header.Set("Authorization", "Bearer "+ck)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	var fin, trace string
	if err := st.db.QueryRow(`SELECT finish_reason, trace FROM requests ORDER BY id DESC LIMIT 1`).Scan(&fin, &trace); err != nil {
		t.Fatal(err)
	}
	if fin != "stop" || !strings.Contains(trace, `"k":"text"`) || !strings.Contains(trace, `"done":true`) {
		t.Fatalf("finish=%q trace=%s", fin, trace)
	}
}
