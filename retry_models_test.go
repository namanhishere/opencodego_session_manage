package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fast(e *tenv) { e.st.SetSetting("retry_backoff_ms", "1") }

func modelOf(body string) string {
	var m map[string]any
	json.Unmarshal([]byte(body), &m)
	s, _ := m["model"].(string)
	return s
}

func TestRetryOnTransientStatus(t *testing.T) {
	var n atomic.Int32
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) < 3 {
			w.WriteHeader(503)
			io.WriteString(w, `{"error":"busy"}`)
			return
		}
		io.WriteString(w, `{"usage":{"prompt_tokens":5,"completion_tokens":1}}`)
	}, "K1")
	fast(e)
	res := e.chat(t, []any{um("user", "hi")})
	if res.status != 200 || e.count() != 3 {
		t.Fatalf("status=%d upstream calls=%d", res.status, e.count())
	}
	if res.hdr.Get("x-proxy-attempts") != "3" {
		t.Fatalf("attempts header %q", res.hdr.Get("x-proxy-attempts"))
	}
	// same key every time, same session header
	for _, r := range e.reqs {
		if r.Header.Get("Authorization") != "Bearer K1" {
			t.Fatal("key changed between retries")
		}
	}
}

func TestRetryGivesUpAndReturnsUpstreamError(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(502)
		io.WriteString(w, `{"error":"bad gateway body"}`)
	}, "K1")
	fast(e)
	res := e.chat(t, []any{um("user", "hi")})
	if res.status != 502 || !strings.Contains(res.body, "bad gateway body") {
		t.Fatalf("%d %s", res.status, res.body)
	}
	if e.count() != 3 { // 1 + retry_max(2)
		t.Fatalf("calls=%d", e.count())
	}
}

func TestRetryDisabled(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, "K1")
	e.st.SetSetting("retry_max", "0")
	if res := e.chat(t, []any{um("user", "hi")}); res.status != 500 || e.count() != 1 {
		t.Fatalf("%d calls=%d", res.status, e.count())
	}
}

func TestNoRetryOnClientError(t *testing.T) {
	for _, code := range []int{400, 401, 403, 422} {
		e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			io.WriteString(w, `{"error":"nope"}`)
		}, "K1")
		fast(e)
		res := e.chat(t, []any{um("user", "hi")})
		if res.status != code || e.count() != 1 || !strings.Contains(res.body, "nope") {
			t.Fatalf("code %d: got %d calls=%d body=%s", code, res.status, e.count(), res.body)
		}
	}
}

func TestRetryAfterHonouredAndCapped(t *testing.T) {
	var n atomic.Int32
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		io.WriteString(w, `{}`)
	}, "K1")
	fast(e)
	start := time.Now()
	if res := e.chat(t, []any{um("user", "hi")}); res.status != 200 {
		t.Fatalf("status %d", res.status)
	}
	if time.Since(start) < 900*time.Millisecond {
		t.Fatalf("Retry-After not honoured: %s", time.Since(start))
	}

	// a huge Retry-After (quota exhausted) must not be waited for or retried
	e2 := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(429)
		io.WriteString(w, `{"error":"quota"}`)
	}, "K1")
	fast(e2)
	start = time.Now()
	res := e2.chat(t, []any{um("user", "hi")})
	if res.status != 429 || e2.count() != 1 || time.Since(start) > 2*time.Second || res.hdr.Get("Retry-After") != "3600" {
		t.Fatalf("%d calls=%d %s hdr=%v", res.status, e2.count(), time.Since(start), res.hdr)
	}
}

func TestRetryOnNetworkError(t *testing.T) {
	var n atomic.Int32
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			hj, _ := w.(http.Hijacker)
			c, _, _ := hj.Hijack()
			c.Close() // connection dropped without a response
			return
		}
		io.WriteString(w, `{}`)
	}, "K1")
	fast(e)
	if res := e.chat(t, []any{um("user", "hi")}); res.status != 200 || e.count() != 2 {
		t.Fatalf("%d calls=%d", res.status, e.count())
	}
}

func TestNetworkErrorExhausted(t *testing.T) {
	e := newEnv(t, nil, "K1")
	fast(e)
	e.up.Close()
	res := e.chat(t, []any{um("user", "hi")})
	if res.status != 502 || !strings.Contains(res.body, "upstream error") {
		t.Fatalf("%d %s", res.status, res.body)
	}
}

func TestRetryResendsFullBodyAndStream(t *testing.T) {
	var n atomic.Int32
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}, "K1")
	fast(e)
	res := e.chat(t, []any{um("user", "hello")}, map[string]any{"stream": true})
	if res.status != 200 || !strings.Contains(res.body, "[DONE]") {
		t.Fatalf("%d %s", res.status, res.body)
	}
	if e.bodies[0] != e.bodies[1] || !strings.Contains(e.bodies[1], "include_usage") {
		t.Fatalf("bodies differ: %q vs %q", e.bodies[0], e.bodies[1])
	}
}

func TestRetryStopsWhenClientCancels(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, "K1")
	e.st.SetSetting("retry_backoff_ms", "5000")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", e.px.URL+"/v1/chat/completions", strings.NewReader(`{"model":"glm-5","messages":[{"role":"user","content":"x"}]}`))
	req.Header.Set("Authorization", "Bearer "+e.ck)
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
	time.Sleep(100 * time.Millisecond)
	if time.Since(start) > 2*time.Second || e.count() != 1 {
		t.Fatalf("kept retrying after cancel: calls=%d", e.count())
	}
}

// ---------- model mapping ----------

func TestParseModelMap(t *testing.T) {
	m, err := parseModelMap("# comment\ngpt-4o = glm-5, kimi-k2 # trailing\n\n  a=b ")
	if err != nil || len(m) != 2 || strings.Join(m["gpt-4o"], "|") != "glm-5|kimi-k2" || m["a"][0] != "b" {
		t.Fatalf("%v %v", m, err)
	}
	for _, bad := range []string{"nothing", "= x", "a = ", "a = , ,"} {
		if _, err := parseModelMap(bad); err == nil {
			t.Fatalf("%q should fail", bad)
		}
	}
}

func TestModelMappingRewritesModel(t *testing.T) {
	e := newEnv(t, nil, "K1")
	e.st.SetSetting("model_map", "gpt-4o = glm-5")
	res := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"model": "gpt-4o", "messages": []any{um("user", "hi")}, "temperature": 0.2}, nil)
	_, body := e.lastReq()
	if res.status != 200 || modelOf(body) != "glm-5" || !strings.Contains(body, `"temperature":0.2`) {
		t.Fatalf("%d %s", res.status, body)
	}
	if res.hdr.Get("x-proxy-model") != "glm-5" {
		t.Fatalf("hdr %v", res.hdr)
	}
	// unmapped models and bodies are untouched byte-for-byte
	raw := `{"model":"other","messages":[{"role":"user","content":"hi"}], "z":1}`
	e.do(t, "POST", "/v1/chat/completions", e.ck, raw, nil)
	if _, b := e.lastReq(); b != raw {
		t.Fatalf("body modified: %s", b)
	}
}

func TestModelFallbackChain(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = b
		io.WriteString(w, `{}`)
	}, "K1")
	var seen []string
	e.handler = func(w http.ResponseWriter, r *http.Request) {
		_, b := e.lastReq()
		m := modelOf(b)
		if r.URL.Path == "/models" {
			io.WriteString(w, `{"data":[]}`)
			return
		}
		seen = append(seen, m)
		switch m {
		case "dead":
			w.WriteHeader(404)
			io.WriteString(w, `{"error":{"message":"model not found"}}`)
		case "flaky":
			w.WriteHeader(503)
		default:
			io.WriteString(w, `{"ok":true}`)
		}
	}
	fast(e)
	e.st.SetSetting("retry_max", "1")
	e.st.SetSetting("model_map", "alias = dead, flaky, good")
	res := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"model": "alias", "messages": []any{um("user", "hi")}}, nil)
	if res.status != 200 || strings.Join(seen, ",") != "dead,flaky,flaky,good" || res.hdr.Get("x-proxy-model") != "good" {
		t.Fatalf("%d seen=%v hdr=%v", res.status, seen, res.hdr)
	}

	// everything fails: the last upstream response is returned
	seen = nil
	e.st.SetSetting("model_map", "alias = dead, flaky")
	res = e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"model": "alias", "messages": []any{um("user", "hi")}}, nil)
	if res.status != 503 {
		t.Fatalf("%d %s", res.status, res.body)
	}
}

func TestGlobalFallbackModel(t *testing.T) {
	e := newEnv(t, nil, "K1")
	e.handler = func(w http.ResponseWriter, r *http.Request) {
		_, b := e.lastReq()
		if modelOf(b) == "mystery" {
			w.WriteHeader(400)
			io.WriteString(w, `{"error":"Unknown model: mystery"}`)
			return
		}
		io.WriteString(w, `{}`)
	}
	fast(e)
	e.st.SetSetting("fallback_model", "glm-5")
	res := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"model": "mystery", "messages": []any{um("user", "hi")}}, nil)
	if res.status != 200 || res.hdr.Get("x-proxy-model") != "glm-5" || res.hdr.Get("x-proxy-attempts") != "2" {
		t.Fatalf("%d %v", res.status, res.hdr)
	}
	// without a fallback the 400 reaches the client
	e.st.SetSetting("fallback_model", "")
	if res := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"model": "mystery", "messages": []any{um("user", "hi")}}, nil); res.status != 400 {
		t.Fatalf("%d", res.status)
	}
}

func TestUnrelated400NotFallenBack(t *testing.T) {
	e := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, `{"error":"messages must be non-empty"}`)
	}, "K1")
	e.st.SetSetting("fallback_model", "glm-5")
	if res := e.chat(t, []any{}); res.status != 400 || e.count() != 1 {
		t.Fatalf("%d calls=%d", res.status, e.count())
	}
}

// ---------- models list ----------

func modelsEnv(t *testing.T, ids ...string) (*tenv, *atomic.Int32) {
	var hits atomic.Int32
	e := newEnv(t, nil, "K1")
	e.handler = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models" {
			hits.Add(1)
			if r.Header.Get("Authorization") != "Bearer K1" {
				w.WriteHeader(401)
				return
			}
			var data []map[string]any
			for _, id := range ids {
				data = append(data, map[string]any{"id": id, "object": "model"})
			}
			json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
			return
		}
		io.WriteString(w, `{}`)
	}
	return e, &hits
}

func TestModelsListFetchedCachedAndAliases(t *testing.T) {
	e, hits := modelsEnv(t, "glm-5", "kimi-k2")
	e.st.SetSetting("model_map", "gpt-4o = glm-5")
	for i := 0; i < 2; i++ {
		res := e.do(t, "GET", "/v1/models", e.ck, nil, nil)
		var out struct{ Data []map[string]any }
		if res.status != 200 || json.Unmarshal([]byte(res.body), &out) != nil || len(out.Data) != 3 {
			t.Fatalf("%d %s", res.status, res.body)
		}
		if out.Data[2]["id"] != "gpt-4o" {
			t.Fatalf("alias missing: %s", res.body)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("expected cached list, upstream hits=%d", hits.Load())
	}
}

func TestModelsRequiresAuthAndKey(t *testing.T) {
	e, _ := modelsEnv(t, "glm-5")
	if res := e.do(t, "GET", "/v1/models", "", nil, nil); res.status != 401 {
		t.Fatalf("%d", res.status)
	}
	e.st.db.Exec(`UPDATE upstream_keys SET enabled=0`)
	if res := e.do(t, "GET", "/v1/models", e.ck, nil, nil); res.status != 503 {
		t.Fatalf("%d", res.status)
	}
}

func TestModelsStaleOnUpstreamFailure(t *testing.T) {
	e, _ := modelsEnv(t, "glm-5")
	if res := e.do(t, "GET", "/v1/models", e.ck, nil, nil); res.status != 200 {
		t.Fatal(res.status)
	}
	e.px.Config.Handler.(*Proxy).catalog.mu.Lock()
	e.px.Config.Handler.(*Proxy).catalog.fetched = time.Now().Add(-time.Hour)
	e.px.Config.Handler.(*Proxy).catalog.mu.Unlock()
	e.up.Close()
	res := e.do(t, "GET", "/v1/models", e.ck, nil, nil)
	if res.status != 200 || res.hdr.Get("x-proxy-models-stale") != "1" || !strings.Contains(res.body, "glm-5") {
		t.Fatalf("%d %v %s", res.status, res.hdr, res.body)
	}
	// never fetched + upstream down -> 502
	e2 := newEnv(t, nil, "K1")
	e2.up.Close()
	if res := e2.do(t, "GET", "/v1/models", e2.ck, nil, nil); res.status != 502 {
		t.Fatalf("%d", res.status)
	}
}

func TestUnlistedModelSkippedWhenFallbackExists(t *testing.T) {
	e, _ := modelsEnv(t, "glm-5")
	e.do(t, "GET", "/v1/models", e.ck, nil, nil) // warm catalog
	e.st.SetSetting("fallback_model", "glm-5")
	n := e.count()
	res := e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"model": "ghost", "messages": []any{um("user", "hi")}}, nil)
	if res.status != 200 || res.hdr.Get("x-proxy-model") != "glm-5" || e.count() != n+1 {
		t.Fatalf("%d calls=%d hdr=%v", res.status, e.count()-n, res.hdr)
	}
	// without a fallback nothing is rewritten, even if unlisted
	e.st.SetSetting("fallback_model", "")
	e.do(t, "POST", "/v1/chat/completions", e.ck, map[string]any{"model": "ghost", "messages": []any{um("user", "hi")}}, nil)
	if _, b := e.lastReq(); modelOf(b) != "ghost" {
		t.Fatalf("%s", b)
	}
}

func TestAdminSettingsValidationAndModels(t *testing.T) {
	e, _ := modelsEnv(t, "glm-5")
	a := NewAdmin(e.st, "pw")
	a.catalog = e.px.Config.Handler.(*Proxy).catalog
	mux := http.NewServeMux()
	a.Routes(mux)
	adm := httptest.NewServer(mux)
	defer adm.Close()
	cookie := &http.Cookie{Name: cookieName, Value: a.sign(time.Now().Add(time.Hour).Unix())}
	send := func(method, path, body string) (int, string) {
		req, _ := http.NewRequest(method, adm.URL+path, strings.NewReader(body))
		req.AddCookie(cookie)
		req.Header.Set("X-Admin", "1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	for body, want := range map[string]int{
		`{"retry_max":"0"}`: 200, `{"retry_max":"11"}`: 400, `{"retry_max":"-1"}`: 400, `{"retry_max":"x"}`: 400,
		`{"retry_backoff_ms":"0"}`: 400, `{"retry_backoff_ms":"250"}`: 200,
		`{"model_map":"a = b, c"}`: 200, `{"model_map":"garbage"}`: 400, `{"fallback_model":"glm-5"}`: 200,
	} {
		if c, b := send("PUT", "/admin/api/settings", body); c != want {
			t.Fatalf("%s -> %d %s", body, c, b)
		}
	}
	if e.st.Setting("model_map", "") != "a = b, c" || e.st.SettingInt("retry_backoff_ms", 0) != 250 {
		t.Fatal("settings not stored")
	}
	if c, b := send("GET", "/admin/api/models?refresh=1", ""); c != 200 || !strings.Contains(b, "glm-5") || !strings.Contains(b, `"a"`) {
		t.Fatalf("%d %s", c, b)
	}
	if c, b := send("GET", "/admin/api/settings", ""); c != 200 || !strings.Contains(b, `"retry_max":"0"`) {
		t.Fatalf("%d %s", c, b)
	}
}
