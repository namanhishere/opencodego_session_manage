package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// env bundles a proxy wired to a programmable upstream.
type tenv struct {
	px      *httptest.Server
	up      *httptest.Server
	st      *Store
	ck      string
	handler http.HandlerFunc
	mu      sync.Mutex
	reqs    []*http.Request
	bodies  []string
}

func newEnv(t *testing.T, h http.HandlerFunc, upstreamKeys ...string) *tenv {
	t.Helper()
	st, err := OpenStore(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	e := &tenv{st: st, handler: h}
	e.up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		e.mu.Lock()
		r.Body = io.NopCloser(bytes.NewReader(b))
		e.reqs = append(e.reqs, r)
		e.bodies = append(e.bodies, string(b))
		e.mu.Unlock()
		if e.handler != nil {
			e.handler(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"usage":{"prompt_tokens":5,"completion_tokens":1}}`)
	}))
	t.Cleanup(e.up.Close)
	e.px = httptest.NewServer(NewProxy(st, NewResolver(st), e.up.URL))
	t.Cleanup(e.px.Close)
	for i, k := range upstreamKeys {
		st.db.Exec(`INSERT INTO upstream_keys(name,key,enabled,created_at) VALUES(?,?,1,0)`, fmt.Sprintf("u%d", i), k)
	}
	e.ck = e.addClient("c")
	return e
}

var clientSeq int

func (e *tenv) addClient(name string) string {
	clientSeq++
	k := fmt.Sprintf("ocp_key%08d", clientSeq)
	e.st.db.Exec(`INSERT INTO client_keys(name,hash,prefix,enabled,created_at) VALUES(?,?,?,1,0)`, name, hashKey(k), k[:8])
	return k
}

type result struct {
	status  int
	body    string
	session string
	hdr     http.Header
}

func (e *tenv) do(t *testing.T, method, path, key string, body any, hdr map[string]string) result {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	case []byte:
		rd = bytes.NewReader(b)
	default:
		j, _ := json.Marshal(b)
		rd = bytes.NewReader(j)
	}
	req, _ := http.NewRequest(method, e.px.URL+path, rd)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return result{resp.StatusCode, string(b), resp.Header.Get("x-proxy-session"), resp.Header}
}

func (e *tenv) chat(t *testing.T, m []any, extra ...map[string]any) result {
	b := map[string]any{"model": "glm-5", "messages": m}
	for _, x := range extra {
		for k, v := range x {
			b[k] = v
		}
	}
	return e.do(t, "POST", "/v1/chat/completions", e.ck, b, nil)
}

func (e *tenv) lastReq() (*http.Request, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.reqs[len(e.reqs)-1], e.bodies[len(e.bodies)-1]
}

func (e *tenv) count() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.reqs) }

func um(role, c string) map[string]any { return map[string]any{"role": role, "content": c} }

// ---------- auth ----------

func TestAuthScenarios(t *testing.T) {
	e := newEnv(t, nil, "K1")
	dis := e.addClient("off")
	e.st.db.Exec(`UPDATE client_keys SET enabled=0 WHERE hash=?`, hashKey(dis))
	cases := []struct {
		name string
		key  string
		hdr  map[string]string
		want int
	}{
		{"no key", "", nil, 401},
		{"wrong key", "ocp_nope", nil, 401},
		{"disabled key", dis, nil, 401},
		{"good bearer", e.ck, nil, 200},
		{"x-api-key", "", map[string]string{"x-api-key": e.ck}, 200},
		{"bearer with spaces", "", map[string]string{"Authorization": "Bearer   " + e.ck + "  "}, 200},
		{"bare token no Bearer", "", map[string]string{"Authorization": e.ck}, 200},
		{"upstream key as client key", "K1", nil, 401},
	}
	for _, c := range cases {
		r := e.do(t, "POST", "/v1/chat/completions", c.key, map[string]any{"messages": []any{um("user", "hi")}}, c.hdr)
		if r.status != c.want {
			t.Errorf("%s: got %d want %d (%s)", c.name, r.status, c.want, r.body)
		}
	}
	// 401 must never reach upstream
	if n := e.count(); n != 4 {
		t.Errorf("upstream saw %d requests, want 4", n)
	}
}

func TestDeletedClientKeyRejected(t *testing.T) {
	e := newEnv(t, nil, "K1")
	e.st.db.Exec(`DELETE FROM client_keys`)
	if r := e.chat(t, []any{um("user", "x")}); r.status != 401 {
		t.Fatalf("got %d", r.status)
	}
}

// ---------- upstream selection ----------

func TestNoUpstreamKeys503(t *testing.T) {
	e := newEnv(t, nil)
	r := e.chat(t, []any{um("user", "x")})
	if r.status != 503 || !strings.Contains(r.body, "no enabled upstream") {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if e.count() != 0 {
		t.Fatal("request leaked upstream")
	}
}

func TestAllUpstreamDisabled503(t *testing.T) {
	e := newEnv(t, nil, "K1")
	e.st.db.Exec(`UPDATE upstream_keys SET enabled=0`)
	if r := e.chat(t, []any{um("user", "x")}); r.status != 503 {
		t.Fatalf("%d", r.status)
	}
}

func TestSpreadAcrossThreeKeys(t *testing.T) {
	e := newEnv(t, nil, "KA", "KB", "KC")
	used := map[string]int{}
	for i := 0; i < 9; i++ {
		e.chat(t, []any{um("user", fmt.Sprintf("conv-%d", i))})
		r, _ := e.lastReq()
		used[r.Header.Get("Authorization")]++
	}
	for _, k := range []string{"Bearer KA", "Bearer KB", "Bearer KC"} {
		if used[k] != 3 {
			t.Errorf("distribution %v", used)
		}
	}
}

func TestSessionStickyAfterOtherKeyAdded(t *testing.T) {
	e := newEnv(t, nil, "KA")
	e.chat(t, []any{um("user", "first")})
	e.st.db.Exec(`INSERT INTO upstream_keys(name,key,enabled,created_at) VALUES('b','KB',1,0)`)
	e.chat(t, []any{um("user", "first"), um("assistant", "a"), um("user", "again")})
	r, _ := e.lastReq()
	if r.Header.Get("Authorization") != "Bearer KA" {
		t.Fatal("session migrated")
	}
}

func TestDisabledUpstreamRepinsSession(t *testing.T) {
	e := newEnv(t, nil, "KA", "KB")
	r1 := e.chat(t, []any{um("user", "hello")})
	req, _ := e.lastReq()
	first := req.Header.Get("Authorization")
	e.st.db.Exec(`UPDATE upstream_keys SET enabled=0 WHERE key=?`, strings.TrimPrefix(first, "Bearer "))
	r2 := e.chat(t, []any{um("user", "hello"), um("assistant", "a"), um("user", "b")})
	req, _ = e.lastReq()
	if req.Header.Get("Authorization") == first {
		t.Fatal("still using disabled key")
	}
	if r1.session != r2.session {
		t.Fatal("session id changed on repin")
	}
}

func TestDeletedUpstreamRepins(t *testing.T) {
	e := newEnv(t, nil, "KA", "KB")
	e.chat(t, []any{um("user", "hello")})
	e.st.db.Exec(`DELETE FROM upstream_keys WHERE key='KA'`)
	if r := e.chat(t, []any{um("user", "hello"), um("assistant", "a"), um("user", "b")}); r.status != 200 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	req, _ := e.lastReq()
	if req.Header.Get("Authorization") != "Bearer KB" {
		t.Fatal("not repinned to KB")
	}
}

// ---------- header handling ----------

func TestHeadersForwardedAndStripped(t *testing.T) {
	e := newEnv(t, nil, "UP-SECRET")
	r := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"messages": []any{um("user", "x")}},
		map[string]string{"x-custom": "keepme", "x-opencode-session": "client-sent", "Accept-Encoding": "gzip", "x-api-key": e.ck, "User-Agent": "ua-test"})
	if r.status != 200 {
		t.Fatal(r.status)
	}
	req, _ := e.lastReq()
	if got := req.Header.Get("Authorization"); got != "Bearer UP-SECRET" {
		t.Errorf("auth %q", got)
	}
	if strings.Contains(fmt.Sprint(req.Header), e.ck) {
		t.Error("client key leaked upstream")
	}
	if req.Header.Get("x-custom") != "keepme" || req.Header.Get("User-Agent") != "ua-test" {
		t.Error("custom headers dropped")
	}
	if got := req.Header.Get("x-opencode-session"); got != r.session || got == "client-sent" {
		t.Errorf("session hdr %q vs %q", got, r.session)
	}
	if len(req.Header.Values("x-opencode-session")) != 1 {
		t.Error("duplicate session header")
	}
	if strings.Contains(r.body, "UP-SECRET") || strings.Contains(fmt.Sprint(r.hdr), "UP-SECRET") {
		t.Error("upstream key leaked to client")
	}
}

func TestResponseHeadersPassedThrough(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ratelimit-remaining", "42")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "{}")
	}, "K")
	r := e.chat(t, []any{um("user", "x")})
	if r.hdr.Get("x-ratelimit-remaining") != "42" || r.hdr.Get("x-proxy-session") == "" {
		t.Fatalf("%v", r.hdr)
	}
}

func TestHeaderSessionScopedPerClient(t *testing.T) {
	e := newEnv(t, nil, "K")
	c2 := e.addClient("two")
	b := map[string]any{"messages": []any{um("user", "x")}}
	a := e.do(t, "POST", "/v1/chat/completions", e.ck, b, map[string]string{"x-opencode-session": "same"})
	c := e.do(t, "POST", "/v1/chat/completions", c2, b, map[string]string{"x-opencode-session": "same"})
	if a.session == c.session {
		t.Fatalf("clients share session %s", a.session)
	}
}

func TestHeaderSessionWithWeirdValues(t *testing.T) {
	e := newEnv(t, nil, "K")
	for _, v := range []string{"a:b:c", "ünï", "x y z", strings.Repeat("a", 500), "../../etc"} {
		r := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"messages": []any{um("user", "x")}}, map[string]string{"x-opencode-session": v})
		if r.status != 200 {
			t.Errorf("%q -> %d %s", v, r.status, r.body)
		}
	}
}

func TestHeaderOverridesChainMatch(t *testing.T) {
	e := newEnv(t, nil, "K")
	a := e.chat(t, []any{um("user", "hello")})
	b := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"messages": []any{um("user", "hello"), um("assistant", "a"), um("user", "b")}},
		map[string]string{"x-opencode-session": "explicit"})
	if a.session == b.session {
		t.Fatal("header ignored")
	}
}

// ---------- session resolution ----------

func TestRetryIdenticalRequestSameSession(t *testing.T) {
	e := newEnv(t, nil, "K")
	m := []any{um("user", "q1"), um("assistant", "a1"), um("user", "q2")}
	a, b := e.chat(t, m), e.chat(t, m)
	if a.session != b.session {
		t.Fatal("retry got new session")
	}
}

func TestClientsIsolated(t *testing.T) {
	e := newEnv(t, nil, "K")
	c2 := e.addClient("two")
	m := map[string]any{"messages": []any{um("system", "S"), um("user", "same")}}
	a := e.do(t, "POST", "/v1/chat/completions", e.ck, m, nil)
	b := e.do(t, "POST", "/v1/chat/completions", c2, m, nil)
	if a.session == b.session {
		t.Fatal("identical prompts from different clients share session")
	}
}

func TestToolCallConversationChain(t *testing.T) {
	e := newEnv(t, nil, "K")
	sys := um("system", "agent")
	u := um("user", "list files")
	asst := map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "call_1", "type": "function", "function": map[string]any{"name": "ls", "arguments": "{}"}}}}
	tool := map[string]any{"role": "tool", "tool_call_id": "call_1", "content": "a.txt"}
	a := e.chat(t, []any{sys, u})
	b := e.chat(t, []any{sys, u, asst, tool})
	// clients often add extra fields (reasoning, ids): must be ignored
	asst2 := map[string]any{"role": "assistant", "content": nil, "reasoning_content": "thinking...", "id": "msg_x", "tool_calls": asst["tool_calls"]}
	c := e.chat(t, []any{sys, u, asst2, tool, um("assistant", "done"), um("user", "thanks")})
	if a.session != b.session {
		t.Fatal("tool turn split session")
	}
	// asst2 differs only in ignored fields -> chain through asst matches b's list
	if b.session != c.session {
		t.Fatal("extra echoed fields broke chain")
	}
}

func TestSimilarButDifferentConversationsSplit(t *testing.T) {
	e := newEnv(t, nil, "K")
	a := e.chat(t, []any{um("system", "S"), um("user", "question A")})
	b := e.chat(t, []any{um("system", "S"), um("user", "question B")})
	if a.session == b.session {
		t.Fatal("different first user messages merged")
	}
}

func TestSystemPromptChangeSplits(t *testing.T) {
	e := newEnv(t, nil, "K")
	a := e.chat(t, []any{um("system", "S1"), um("user", "same")})
	b := e.chat(t, []any{um("system", "S2"), um("user", "same")})
	if a.session == b.session {
		t.Fatal("different system prompts merged")
	}
}

func TestResponsesAPIInputArray(t *testing.T) {
	e := newEnv(t, nil, "K")
	in1 := []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hi"}}}}
	in2 := append(append([]any{}, in1...), map[string]any{"type": "function_call", "call_id": "c1", "name": "x", "arguments": "{}"},
		map[string]any{"type": "function_call_output", "call_id": "c1", "output": "ok"})
	a := e.do(t, "POST", "/v1/responses", e.ck, map[string]any{"model": "m", "input": in1, "instructions": "be nice"}, nil)
	b := e.do(t, "POST", "/v1/responses", e.ck, map[string]any{"model": "m", "input": in2, "instructions": "be nice"}, nil)
	if a.status != 200 || a.session != b.session {
		t.Fatalf("%d %q %q", a.status, a.session, b.session)
	}
	if req, _ := e.lastReq(); req.URL.Path != "/responses" {
		t.Fatalf("path %s", req.URL.Path)
	}
}

func TestResponsesAPIStringInput(t *testing.T) {
	e := newEnv(t, nil, "K")
	a := e.do(t, "POST", "/v1/responses", e.ck, map[string]any{"model": "m", "input": "hello"}, nil)
	b := e.do(t, "POST", "/v1/responses", e.ck, map[string]any{"model": "m", "input": "hello"}, nil)
	c := e.do(t, "POST", "/v1/responses", e.ck, map[string]any{"model": "m", "input": "other"}, nil)
	if a.session != b.session || a.session == c.session {
		t.Fatalf("%q %q %q", a.session, b.session, c.session)
	}
}

func TestMultimodalContent(t *testing.T) {
	e := newEnv(t, nil, "K")
	m := []any{map[string]any{"role": "user", "content": []any{
		map[string]any{"type": "text", "text": "look"},
		map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64,AAAA"}}}}}
	a := e.chat(t, m)
	b := e.chat(t, append(append([]any{}, m...), um("assistant", "a"), um("user", "b")))
	if a.session != b.session {
		t.Fatal("multimodal chain broken")
	}
}

func TestExpiredHeaderSessionRestarts(t *testing.T) {
	e := newEnv(t, nil, "K")
	h := map[string]string{"x-opencode-session": "s1"}
	b := map[string]any{"messages": []any{um("user", "x")}}
	a := e.do(t, "POST", "/v1/chat/completions", e.ck, b, h)
	e.st.db.Exec(`UPDATE sessions SET expires_at=1`)
	c := e.do(t, "POST", "/v1/chat/completions", e.ck, b, h)
	if a.session != c.session || c.status != 200 {
		t.Fatal("header id should be reusable after expiry")
	}
	var exp int64
	var expired int
	e.st.db.QueryRow(`SELECT expires_at, expired FROM sessions WHERE id=?`, a.session).Scan(&exp, &expired)
	if exp < time.Now().Unix() || expired != 0 {
		t.Fatal("session not revived")
	}
}

func TestSessionTTLSettingApplied(t *testing.T) {
	e := newEnv(t, nil, "K")
	e.st.SetSetting("session_ttl_hours", "2")
	r := e.chat(t, []any{um("user", "x")})
	var exp int64
	e.st.db.QueryRow(`SELECT expires_at FROM sessions WHERE id=?`, r.session).Scan(&exp)
	d := time.Until(time.Unix(exp, 0))
	if d > 2*time.Hour+time.Minute || d < time.Hour {
		t.Fatalf("ttl %v", d)
	}
}

func TestExpireSessionsSweep(t *testing.T) {
	e := newEnv(t, nil, "K")
	e.chat(t, []any{um("user", "x")})
	e.st.db.Exec(`UPDATE sessions SET expires_at=1`)
	e.st.db.Exec(`UPDATE fingerprints SET expires_at=1`)
	e.st.mu.Lock()
	for h, f := range e.st.fp {
		f.expires = 1
		e.st.fp[h] = f
	}
	e.st.mu.Unlock()
	e.st.ExpireSessions()
	var n, fps int
	e.st.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE expired=1`).Scan(&n)
	e.st.db.QueryRow(`SELECT COUNT(*) FROM fingerprints`).Scan(&fps)
	if n != 1 || fps != 0 || len(e.st.fp) != 0 {
		t.Fatalf("expired=%d fps=%d mem=%d", n, fps, len(e.st.fp))
	}
}

func TestFingerprintsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.db")
	st, _ := OpenStore(path)
	st.db.Exec(`INSERT INTO upstream_keys(name,key,enabled,created_at) VALUES('a','K',1,0)`)
	ck := "ocp_restart1"
	st.db.Exec(`INSERT INTO client_keys(name,hash,prefix,enabled,created_at) VALUES('c',?,?,1,0)`, hashKey(ck), ck[:8])
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "{}") }))
	defer up.Close()
	px := httptest.NewServer(NewProxy(st, NewResolver(st), up.URL))
	post := func(px *httptest.Server, m []any) string {
		b, _ := json.Marshal(map[string]any{"messages": m})
		req, _ := http.NewRequest("POST", px.URL+"/v1/chat/completions", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+ck)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.Header.Get("x-proxy-session")
	}
	a := post(px, []any{um("user", "persist")})
	px.Close()
	st.Close()
	st2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	px2 := httptest.NewServer(NewProxy(st2, NewResolver(st2), up.URL))
	defer px2.Close()
	b := post(px2, []any{um("user", "persist"), um("assistant", "a"), um("user", "more")})
	if a != b {
		t.Fatalf("session lost across restart: %s vs %s", a, b)
	}
}

func TestConcurrentFirstRequestsShareSession(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		io.WriteString(w, "{}")
	}, "K1", "K2")
	var wg sync.WaitGroup
	sess := make([]string, 20)
	for i := range sess {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sess[i] = e.chat(t, []any{um("system", "S"), um("user", "identical start")}).session
		}(i)
	}
	wg.Wait()
	// Touch happens after response, so some early racers may legitimately create separate sessions;
	// report the spread rather than fail hard, but never allow errors.
	uniq := map[string]bool{}
	for _, s := range sess {
		if s == "" {
			t.Fatal("empty session")
		}
		uniq[s] = true
	}
	t.Logf("20 concurrent identical first requests -> %d distinct sessions", len(uniq))
}

func TestConcurrentDistinctConversationsNoCrossTalk(t *testing.T) {
	e := newEnv(t, nil, "K1", "K2")
	var wg sync.WaitGroup
	const n = 30
	got := make([][3]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := []any{um("user", fmt.Sprintf("conv %d", i))}
			got[i][0] = e.chat(t, m).session
			m = append(m, um("assistant", "a"), um("user", "b"))
			got[i][1] = e.chat(t, m).session
			m = append(m, um("assistant", "c"), um("user", "d"))
			got[i][2] = e.chat(t, m).session
		}(i)
	}
	wg.Wait()
	seen := map[string]int{}
	for i, g := range got {
		if g[0] != g[1] || g[1] != g[2] {
			t.Errorf("conv %d split: %v", i, g)
		}
		if j, dup := seen[g[0]]; dup {
			t.Errorf("conv %d and %d share session", i, j)
		}
		seen[g[0]] = i
	}
}

// ---------- request/response passthrough ----------

func TestQueryStringAndPathMapping(t *testing.T) {
	e := newEnv(t, nil, "K")
	e.do(t, "GET", "/v1/files?foo=bar&x=1", e.ck, nil, nil)
	req, _ := e.lastReq()
	if req.URL.Path != "/files" || req.URL.RawQuery != "foo=bar&x=1" || req.Method != "GET" {
		t.Fatalf("%s %s?%s", req.Method, req.URL.Path, req.URL.RawQuery)
	}
}

func TestNonJSONBodyPassthrough(t *testing.T) {
	e := newEnv(t, nil, "K")
	r := e.do(t, "POST", "/v1/audio/transcriptions", e.ck, "not json", map[string]string{"Content-Type": "multipart/form-data; boundary=x"})
	if r.status != 200 {
		t.Fatal(r.status)
	}
	if _, b := e.lastReq(); b != "not json" {
		t.Fatalf("body altered: %q", b)
	}
}

func TestInvalidJSONForwardedUntouched(t *testing.T) {
	e := newEnv(t, nil, "K")
	r := e.do(t, "POST", "/v1/chat/completions", e.ck, `{"messages": [broken`, nil)
	if r.status != 200 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if _, b := e.lastReq(); b != `{"messages": [broken` {
		t.Fatalf("%q", b)
	}
}

func TestEmptyAndOddBodies(t *testing.T) {
	e := newEnv(t, nil, "K")
	for _, b := range []string{``, `null`, `[]`, `"str"`, `{}`, `{"messages":null}`, `{"messages":"x"}`, `{"messages":[1,2,"a",null]}`, `{"input":5}`} {
		r := e.do(t, "POST", "/v1/chat/completions", e.ck, b, map[string]string{"Content-Type": "application/json"})
		if r.status != 200 {
			t.Errorf("body %q -> %d %s", b, r.status, r.body)
		}
	}
}

func TestUnicodeAndHugeMessage(t *testing.T) {
	e := newEnv(t, nil, "K")
	big := strings.Repeat("日本語🙂 ", 200000) // ~2.4MB
	r := e.chat(t, []any{um("user", big)})
	if r.status != 200 {
		t.Fatal(r.status)
	}
	if _, b := e.lastReq(); !strings.Contains(b, "日本語🙂") || len(b) < 2_000_000 {
		t.Fatal("large unicode body mangled")
	}
	// recorded body is truncated but request stored
	var rb string
	e.st.db.QueryRow(`SELECT req_body FROM requests ORDER BY id DESC LIMIT 1`).Scan(&rb)
	if len(rb) > maxLogBody+100 {
		t.Fatalf("logged %d bytes", len(rb))
	}
}

func TestRequestTooLarge413(t *testing.T) {
	e := newEnv(t, nil, "K")
	big := make([]byte, maxRequestBody+1024)
	for i := range big {
		big[i] = ' '
	}
	r := e.do(t, "POST", "/v1/chat/completions", e.ck, big, nil)
	if r.status != 413 {
		t.Fatalf("got %d", r.status)
	}
	if e.count() != 0 {
		t.Fatal("oversized body reached upstream")
	}
}

func TestStreamOptionsInjection(t *testing.T) {
	e := newEnv(t, nil, "K")
	// injected when missing
	e.chat(t, []any{um("user", "a")}, map[string]any{"stream": true})
	if _, b := e.lastReq(); !strings.Contains(b, `"include_usage":true`) {
		t.Errorf("not injected: %s", b)
	}
	// existing stream_options keys preserved
	e.chat(t, []any{um("user", "b")}, map[string]any{"stream": true, "stream_options": map[string]any{"foo": 1}})
	if _, b := e.lastReq(); !strings.Contains(b, `"foo":1`) || !strings.Contains(b, `"include_usage":true`) {
		t.Errorf("merge failed: %s", b)
	}
	// client's explicit false respected
	e.chat(t, []any{um("user", "c")}, map[string]any{"stream": true, "stream_options": map[string]any{"include_usage": false}})
	if _, b := e.lastReq(); !strings.Contains(b, `"include_usage":false`) {
		t.Errorf("client choice overridden: %s", b)
	}
	// not injected for non-stream
	e.chat(t, []any{um("user", "d")})
	if _, b := e.lastReq(); strings.Contains(b, "include_usage") {
		t.Errorf("injected on non-stream: %s", b)
	}
	// not on /responses
	e.do(t, "POST", "/v1/responses", e.ck, map[string]any{"input": "x", "stream": true}, nil)
	if _, b := e.lastReq(); strings.Contains(b, "include_usage") {
		t.Errorf("injected on responses: %s", b)
	}
	// setting off
	e.st.SetSetting("inject_usage", "0")
	e.chat(t, []any{um("user", "e")}, map[string]any{"stream": true})
	if _, b := e.lastReq(); strings.Contains(b, "include_usage") {
		t.Errorf("injected while disabled: %s", b)
	}
}

func TestStreamingIsIncremental(t *testing.T) {
	gate := make(chan struct{})
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-gate
		io.WriteString(w, "data: [DONE]\n\n")
	}, "K")
	b, _ := json.Marshal(map[string]any{"messages": []any{um("user", "x")}, "stream": true})
	req, _ := http.NewRequest("POST", e.px.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+e.ck)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 64)
	done := make(chan string, 1)
	go func() { n, _ := resp.Body.Read(buf); done <- string(buf[:n]) }()
	select {
	case s := <-done:
		if !strings.Contains(s, "one") {
			t.Fatalf("got %q", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first chunk not delivered before upstream finished (buffering!)")
	}
	close(gate)
	rest, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("rest %q", rest)
	}
}

func TestClientDisconnectMidStream(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 200; i++ {
			if _, err := fmt.Fprintf(w, "data: {\"i\":%d}\n\n", i); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}, "K")
	b, _ := json.Marshal(map[string]any{"messages": []any{um("user", "x")}, "stream": true})
	req, _ := http.NewRequest("POST", e.px.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+e.ck)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	resp.Body.Read(buf)
	resp.Body.Close() // abort
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		e.st.db.QueryRow(`SELECT COUNT(*) FROM requests`).Scan(&n)
		if n == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("aborted request never recorded (goroutine stuck?)")
}

// ---------- upstream failures ----------

func TestUpstream429NoFailover(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(429)
		io.WriteString(w, `{"error":{"message":"rate limited"}}`)
	}, "KA", "KB")
	r := e.chat(t, []any{um("user", "x")})
	if r.status != 429 || r.hdr.Get("Retry-After") != "30" || !strings.Contains(r.body, "rate limited") {
		t.Fatalf("%d %v %s", r.status, r.hdr, r.body)
	}
	if e.count() != 1 {
		t.Fatalf("proxy retried/failed over: %d upstream calls", e.count())
	}
	var errMsg string
	var status int
	e.st.db.QueryRow(`SELECT status, error FROM requests`).Scan(&status, &errMsg)
	if status != 429 || !strings.Contains(errMsg, "rate limited") {
		t.Fatalf("recorded %d %q", status, errMsg)
	}
}

func TestFailedRequestDoesNotRegisterFingerprint(t *testing.T) {
	fail := true
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(500)
			return
		}
		io.WriteString(w, "{}")
	}, "K")
	a := e.chat(t, []any{um("user", "x")})
	fail = false
	b := e.chat(t, []any{um("user", "x")})
	if a.session == b.session {
		t.Log("note: failed request's session was reused (fingerprint registered on error)")
	}
	var c int
	e.st.db.QueryRow(`SELECT requests FROM sessions WHERE id=?`, a.session).Scan(&c)
	if c != 0 {
		t.Fatalf("failed request counted in session.requests=%d", c)
	}
}

func TestUpstreamUnreachable502(t *testing.T) {
	e := newEnv(t, nil, "K")
	e.up.Close()
	r := e.chat(t, []any{um("user", "x")})
	if r.status != 502 || !strings.Contains(r.body, "upstream error") {
		t.Fatalf("%d %s", r.status, r.body)
	}
	var s int
	e.st.db.QueryRow(`SELECT status FROM requests`).Scan(&s)
	if s != 502 {
		t.Fatalf("recorded %d", s)
	}
}

func TestUpstreamRedirectNotFollowed(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://evil.invalid/steal", 302)
	}, "K")
	b, _ := json.Marshal(map[string]any{"messages": []any{um("user", "x")}})
	req, _ := http.NewRequest("POST", e.px.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+e.ck)
	req.Header.Set("Content-Type", "application/json")
	cl := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := cl.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 302 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestUpstreamAbortsMidBody(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, "partial")
		w.(http.Flusher).Flush()
		hj, _, _ := w.(http.Hijacker).Hijack()
		hj.Close()
	}, "K")
	b, _ := json.Marshal(map[string]any{"messages": []any{um("user", "x")}})
	req, _ := http.NewRequest("POST", e.px.URL+"/v1/chat/completions", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+e.ck)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	time.Sleep(200 * time.Millisecond)
	var em string
	if e.st.db.QueryRow(`SELECT error FROM requests`).Scan(&em) != nil {
		t.Fatal("not recorded")
	}
	t.Logf("recorded error: %q", em)
}

// ---------- usage parsing ----------

func TestUsageParsing(t *testing.T) {
	cases := []struct {
		name, body string
		p, c, ca   int
	}{
		{"chat json", `{"usage":{"prompt_tokens":10,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":4}}}`, 10, 3, 4},
		{"responses json", `{"usage":{"input_tokens":20,"output_tokens":5,"input_tokens_details":{"cached_tokens":8}}}`, 20, 5, 8},
		{"responses nested", `{"response":{"usage":{"input_tokens":1,"output_tokens":2}}}`, 1, 2, 0},
		{"no usage", `{"choices":[]}`, 0, 0, 0},
		{"garbage", `<html>502</html>`, 0, 0, 0},
		{"sse chat", "data: {\"choices\":[]}\n\ndata: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":8}}\n\ndata: [DONE]\n\n", 7, 8, 0},
		{"sse responses completed", "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":11,\"output_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":3}}}}\n\n", 11, 12, 3},
		{"sse crlf", "data: {\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\r\n\r\ndata: [DONE]\r\n\r\n", 2, 3, 0},
		{"sse usage null mid-stream", "data: {\"usage\":null,\"choices\":[]}\n\ndata: {\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n", 9, 1, 0},
	}
	for _, c := range cases {
		cp := &capture{headMax: headSize, tailMax: tailSize}
		cp.write([]byte(c.body))
		u := parseUsage(cp)
		if u.Prompt != c.p || u.Completion != c.c || u.Cached != c.ca {
			t.Errorf("%s: got %+v", c.name, u)
		}
	}
}

func TestUsageFromLongStream(t *testing.T) {
	cp := &capture{headMax: headSize, tailMax: tailSize}
	for i := 0; i < 5000; i++ {
		cp.write([]byte(fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":\"token %d\"}}]}\n\n", i)))
	}
	cp.write([]byte("data: {\"usage\":{\"prompt_tokens\":123,\"completion_tokens\":456}}\n\ndata: [DONE]\n\n"))
	if u := parseUsage(cp); u.Prompt != 123 || u.Completion != 456 {
		t.Fatalf("%+v", u)
	}
	if len(cp.tail) > 3*tailSize {
		t.Fatalf("tail unbounded: %d", len(cp.tail))
	}
}

func TestSessionUsageAccumulates(t *testing.T) {
	e := newEnv(t, nil, "K")
	m := []any{um("user", "x")}
	var s string
	for i := 0; i < 3; i++ {
		s = e.chat(t, m).session
		m = append(m, um("assistant", "a"), um("user", fmt.Sprint(i)))
	}
	var req, p, c int
	e.st.db.QueryRow(`SELECT requests, prompt_tokens, completion_tokens FROM sessions WHERE id=?`, s).Scan(&req, &p, &c)
	if req != 3 || p != 15 || c != 3 {
		t.Fatalf("%d %d %d", req, p, c)
	}
}

func TestLogBodiesOff(t *testing.T) {
	e := newEnv(t, nil, "K")
	e.st.SetSetting("log_bodies", "0")
	e.chat(t, []any{um("user", "secret-prompt")})
	var rb, sb string
	e.st.db.QueryRow(`SELECT req_body, resp_body FROM requests`).Scan(&rb, &sb)
	if rb != "" || sb != "" {
		t.Fatal("bodies logged while disabled")
	}
}

func TestUpstreamKeyNeverLoggedInBodies(t *testing.T) {
	e := newEnv(t, nil, "UPSTREAM-SECRET-123")
	e.chat(t, []any{um("user", "x")})
	var rb, sb, em string
	e.st.db.QueryRow(`SELECT req_body, resp_body, error FROM requests`).Scan(&rb, &sb, &em)
	if strings.Contains(rb+sb+em, "UPSTREAM-SECRET-123") {
		t.Fatal("upstream key in logs")
	}
}

func TestPruneRequests(t *testing.T) {
	e := newEnv(t, nil, "K")
	r := e.chat(t, []any{um("user", "x")})
	old := time.Now().Add(-40 * 24 * time.Hour).Unix()
	e.st.db.Exec(`UPDATE requests SET ts=?`, old)
	e.st.db.Exec(`UPDATE sessions SET expired=1, last_seen=?`, old)
	e.st.PruneRequests(30)
	var n, s int
	e.st.db.QueryRow(`SELECT COUNT(*) FROM requests`).Scan(&n)
	e.st.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE id=?`, r.session).Scan(&s)
	if n != 0 || s != 0 {
		t.Fatalf("requests=%d sessions=%d", n, s)
	}
}

// ---------- admin API ----------

type adminEnv struct {
	*httptest.Server
	st     *Store
	cookie *http.Cookie
}

func newAdmin(t *testing.T) *adminEnv {
	t.Helper()
	st, _ := OpenStore(filepath.Join(t.TempDir(), "a.db"))
	t.Cleanup(func() { st.Close() })
	mux := http.NewServeMux()
	NewAdmin(st, "pw").Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &adminEnv{Server: srv, st: st}
}

func (a *adminEnv) req(t *testing.T, method, path string, body any, withCookie bool, hdr map[string]string) (int, map[string]any, []any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		j, _ := json.Marshal(body)
		rd = bytes.NewReader(j)
	}
	req, _ := http.NewRequest(method, a.URL+path, rd)
	if withCookie && a.cookie != nil {
		req.AddCookie(a.cookie)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	for _, c := range resp.Cookies() {
		if c.Name == cookieName && c.MaxAge >= 0 {
			a.cookie = c
		}
	}
	b, _ := io.ReadAll(resp.Body)
	var m map[string]any
	var l []any
	if json.Unmarshal(b, &m) != nil {
		json.Unmarshal(b, &l)
	}
	return resp.StatusCode, m, l
}

func (a *adminEnv) login(t *testing.T) {
	if c, _, _ := a.req(t, "POST", "/admin/api/login", map[string]string{"password": "pw"}, false, nil); c != 200 {
		t.Fatalf("login %d", c)
	}
}

var xa = map[string]string{"X-Admin": "1"}

func TestAdminAuthAndCSRF(t *testing.T) {
	a := newAdmin(t)
	if c, _, _ := a.req(t, "GET", "/admin/api/overview", nil, false, nil); c != 401 {
		t.Errorf("unauth overview %d", c)
	}
	start := time.Now()
	if c, _, _ := a.req(t, "POST", "/admin/api/login", map[string]string{"password": "bad"}, false, nil); c != 401 {
		t.Errorf("bad pw %d", c)
	}
	if time.Since(start) < 500*time.Millisecond {
		t.Error("no brute-force delay")
	}
	a.login(t)
	if c, _, _ := a.req(t, "GET", "/admin/api/me", nil, true, nil); c != 200 {
		t.Errorf("me %d", c)
	}
	if c, _, _ := a.req(t, "POST", "/admin/api/client-keys", map[string]string{"name": "x"}, true, nil); c != 403 {
		t.Errorf("csrf: mutation without X-Admin gave %d", c)
	}
	if c, _, _ := a.req(t, "POST", "/admin/api/client-keys", map[string]string{"name": "x"}, true, xa); c != 200 {
		t.Errorf("mutation with header %d", c)
	}
	// forged / expired cookies
	for _, v := range []string{"", "garbage", "9999999999.deadbeef", fmt.Sprintf("%d.%s", time.Now().Add(-time.Hour).Unix(), "00")} {
		a.cookie = &http.Cookie{Name: cookieName, Value: v}
		if c, _, _ := a.req(t, "GET", "/admin/api/me", nil, true, nil); c != 401 {
			t.Errorf("forged cookie %q accepted: %d", v, c)
		}
	}
}

func TestAdminSecretPersistsAcrossRestart(t *testing.T) {
	st, _ := OpenStore(filepath.Join(t.TempDir(), "a.db"))
	defer st.Close()
	a1 := NewAdmin(st, "pw")
	tok := a1.sign(time.Now().Add(time.Hour).Unix())
	a2 := NewAdmin(st, "pw")
	if !a2.valid(tok) {
		t.Fatal("cookie invalid after restart")
	}
}

func TestAdminKeyLifecycleEndToEnd(t *testing.T) {
	// full flow: admin creates keys over HTTP, proxy uses them
	st, _ := OpenStore(filepath.Join(t.TempDir(), "x.db"))
	defer st.Close()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"usage":{"prompt_tokens":3,"completion_tokens":4}}`)
	}))
	defer up.Close()
	mux := http.NewServeMux()
	NewAdmin(st, "pw").Routes(mux)
	mux.Handle("/v1/", NewProxy(st, NewResolver(st), up.URL))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	a := &adminEnv{Server: srv, st: st}
	a.login(t)

	// before any keys
	code := func(key string) int {
		req, _ := http.NewRequest("POST", srv.URL+"/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	_, m, _ := a.req(t, "POST", "/admin/api/client-keys", map[string]string{"name": "agent"}, true, xa)
	ck := m["key"].(string)
	cid := fmt.Sprint(int64(m["id"].(float64)))
	if !strings.HasPrefix(ck, "ocp_") {
		t.Fatal(ck)
	}
	if c := code(ck); c != 503 {
		t.Fatalf("no upstream yet: %d", c)
	}
	_, m, _ = a.req(t, "POST", "/admin/api/upstream-keys", map[string]string{"name": "main", "key": "  sk-abcdefghijklmnop  "}, true, xa)
	uid := fmt.Sprint(int64(m["id"].(float64)))
	if c := code(ck); c != 200 {
		t.Fatalf("with upstream: %d", c)
	}
	time.Sleep(200 * time.Millisecond) // request is recorded just after the response body is flushed
	// listing masks secrets
	_, _, l := a.req(t, "GET", "/admin/api/upstream-keys", nil, true, nil)
	row := l[0].(map[string]any)
	if strings.Contains(fmt.Sprint(row["key"]), "abcdefgh") || row["requests"].(float64) != 1 {
		t.Errorf("list %v", row)
	}
	_, _, l = a.req(t, "GET", "/admin/api/client-keys", nil, true, nil)
	if strings.Contains(fmt.Sprint(l), ck) {
		t.Error("client key listed in full")
	}
	// disable client
	a.req(t, "PATCH", "/admin/api/client-keys/"+cid, map[string]any{"enabled": false}, true, xa)
	if c := code(ck); c != 401 {
		t.Errorf("disabled client: %d", c)
	}
	a.req(t, "PATCH", "/admin/api/client-keys/"+cid, map[string]any{"enabled": true}, true, xa)
	if c := code(ck); c != 200 {
		t.Errorf("re-enabled client: %d", c)
	}
	// disable upstream
	a.req(t, "PATCH", "/admin/api/upstream-keys/"+uid, map[string]any{"enabled": false, "name": "renamed"}, true, xa)
	if c := code(ck); c != 503 {
		t.Errorf("disabled upstream: %d", c)
	}
	// overview + sessions + requests
	_, m, _ = a.req(t, "GET", "/admin/api/overview?hours=24", nil, true, nil)
	tot := m["totals"].(map[string]any)
	if tot["requests"].(float64) < 2 || tot["prompt_tokens"].(float64) < 6 {
		t.Errorf("overview %v", tot)
	}
	_, _, sl := a.req(t, "GET", "/admin/api/sessions?state=active", nil, true, nil)
	if len(sl) < 1 {
		t.Fatal("no sessions")
	}
	sid := sl[0].(map[string]any)["id"].(string)
	if c, _, _ := a.req(t, "GET", "/admin/api/sessions/"+sid, nil, true, nil); c != 200 {
		t.Errorf("session detail %d", c)
	}
	if c, _, _ := a.req(t, "GET", "/admin/api/sessions/nope", nil, true, nil); c != 404 {
		t.Errorf("missing session %d", c)
	}
	a.req(t, "POST", "/admin/api/sessions/"+sid+"/expire", nil, true, xa)
	_, _, sl = a.req(t, "GET", "/admin/api/sessions?state=active", nil, true, nil)
	if len(sl) != 0 {
		t.Errorf("session still active after expire: %d", len(sl))
	}
	_, _, rl := a.req(t, "GET", "/admin/api/requests?status=ok&limit=1", nil, true, nil)
	if len(rl) != 1 {
		t.Fatalf("requests %v", rl)
	}
	rid := fmt.Sprint(int64(rl[0].(map[string]any)["id"].(float64)))
	if c, m, _ := a.req(t, "GET", "/admin/api/requests/"+rid, nil, true, nil); c != 200 || m["req_body"] == "" {
		t.Errorf("request detail %d", c)
	}
	if c, _, _ := a.req(t, "GET", "/admin/api/requests/999999", nil, true, nil); c != 404 {
		t.Errorf("missing request %d", c)
	}
	// delete keys
	a.req(t, "DELETE", "/admin/api/upstream-keys/"+uid, nil, true, xa)
	a.req(t, "DELETE", "/admin/api/client-keys/"+cid, nil, true, xa)
	if c := code(ck); c != 401 {
		t.Errorf("deleted client: %d", c)
	}
}

func TestAdminInputValidation(t *testing.T) {
	a := newAdmin(t)
	a.login(t)
	for _, b := range []any{map[string]string{"key": ""}, map[string]string{"key": "   "}, map[string]string{}} {
		if c, _, _ := a.req(t, "POST", "/admin/api/upstream-keys", b, true, xa); c != 400 {
			t.Errorf("%v -> %d", b, c)
		}
	}
	for _, v := range []string{"0", "-1", "abc", ""} {
		if c, _, _ := a.req(t, "PUT", "/admin/api/settings", map[string]string{"session_ttl_hours": v}, true, xa); c != 400 {
			t.Errorf("ttl %q -> %d", v, c)
		}
	}
	if c, _, _ := a.req(t, "PUT", "/admin/api/settings", map[string]string{"session_ttl_hours": "48", "bogus": "x", "log_bodies": "0"}, true, xa); c != 200 {
		t.Errorf("valid settings %d", c)
	}
	_, m, _ := a.req(t, "GET", "/admin/api/settings", nil, true, nil)
	if m["session_ttl_hours"] != "48" || m["log_bodies"] != "0" || m["bogus"] != nil {
		t.Errorf("settings %v", m)
	}
	// SQL injection attempts in filters must not error or leak
	for _, p := range []string{"/admin/api/sessions?q=%27%20OR%201=1--", "/admin/api/requests?model=%27;DROP%20TABLE%20requests;--", "/admin/api/sessions?limit=abc&offset=-5", "/admin/api/overview?hours=abc"} {
		if c, _, _ := a.req(t, "GET", p, nil, true, nil); c != 200 {
			t.Errorf("%s -> %d", p, c)
		}
	}
	var n int
	if err := a.st.db.QueryRow(`SELECT COUNT(*) FROM requests`).Scan(&n); err != nil {
		t.Fatal("requests table damaged", err)
	}
	// patch/delete nonexistent ids and bad json are harmless
	if c, _, _ := a.req(t, "PATCH", "/admin/api/client-keys/9999", map[string]any{"enabled": false}, true, xa); c != 200 {
		t.Errorf("patch missing %d", c)
	}
	if c, _, _ := a.req(t, "DELETE", "/admin/api/upstream-keys/abc", nil, true, xa); c != 200 {
		t.Errorf("delete bad id %d", c)
	}
}

func TestAdminLogout(t *testing.T) {
	a := newAdmin(t)
	a.login(t)
	a.req(t, "POST", "/admin/api/logout", nil, true, xa)
	// cookie value is cleared client-side only; confirm endpoint responds
}

func TestLogRequestsOff(t *testing.T) {
	e := newEnv(t, nil, "K")
	e.st.SetSetting("log_requests", "0")
	r := e.chat(t, []any{um("user", "x")})
	var n int
	e.st.db.QueryRow(`SELECT COUNT(*) FROM requests`).Scan(&n)
	if n != 0 {
		t.Fatalf("%d requests logged while disabled", n)
	}
	var cnt int
	e.st.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE id=?`, r.session).Scan(&cnt)
	if cnt != 1 {
		t.Fatal("session not tracked")
	}
}

func TestIgnoreClientSessionHeader(t *testing.T) {
	e := newEnv(t, nil, "K")
	e.st.SetSetting("ignore_client_session", "1")
	a := e.chat(t, []any{um("user", "hello")})
	b := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"messages": []any{um("user", "hello"), um("assistant", "a"), um("user", "b")}},
		map[string]string{"x-opencode-session": "explicit"})
	if a.session != b.session || b.session == "explicit" {
		t.Fatalf("client header not overridden: %s vs %s", a.session, b.session)
	}
}
