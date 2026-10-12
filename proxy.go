package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

var errNoUpstream = errors.New("no enabled upstream key configured")

func randRead(b []byte) { rand.Read(b) }

func hashKey(k string) string {
	h := sha256.Sum256([]byte(k))
	return hex.EncodeToString(h[:])
}

type Usage struct{ Prompt, Completion, Cached int }

const (
	maxRequestBody = 64 << 20
	maxLogBody     = 64 << 10 // per request/response stored for tracing
	tailSize       = 32 << 10 // kept to find the usage block at the end of an SSE stream
	headSize       = 1 << 20  // kept to parse non-streaming JSON responses
)

var hopHeaders = map[string]bool{
	"Connection": true, "Proxy-Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true,
	"Proxy-Authorization": true, "Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true,
	"Content-Length": true, "Host": true, "Authorization": true, "X-Api-Key": true, "Accept-Encoding": true,
}

type Proxy struct {
	st       *Store
	rs       *Resolver
	upstream string // e.g. https://opencode.ai/zen/go/v1
	client   *http.Client
}

func NewProxy(st *Store, rs *Resolver, upstream string) *Proxy {
	return &Proxy{
		st: st, rs: rs, upstream: strings.TrimRight(upstream, "/"),
		client: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment, MaxIdleConnsPerHost: 64,
				IdleConnTimeout: 90 * time.Second, ResponseHeaderTimeout: 10 * time.Minute,
				ForceAttemptHTTP2: true,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func apiError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": msg, "type": "proxy_error"}})
}

func (p *Proxy) authClient(r *http.Request) (int64, bool) {
	tok := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if tok == "" {
		tok = r.Header.Get("x-api-key")
	}
	if tok == "" {
		return 0, false
	}
	var id int64
	err := p.st.db.QueryRow(`SELECT id FROM client_keys WHERE hash=? AND enabled=1`, hashKey(tok)).Scan(&id)
	return id, err == nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	clientID, ok := p.authClient(r)
	if !ok {
		apiError(w, http.StatusUnauthorized, "invalid or missing proxy API key")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		apiError(w, http.StatusRequestEntityTooLarge, "request body too large or unreadable")
		return
	}

	var parsed map[string]any
	var model string
	var stream bool
	if len(body) > 0 && strings.Contains(r.Header.Get("Content-Type"), "json") {
		if json.Unmarshal(body, &parsed) == nil {
			model, _ = parsed["model"].(string)
			stream, _ = parsed["stream"].(bool)
		}
	}
	fp := ComputeFingerprints(clientID, parsed)

	sess, err := p.rs.Resolve(clientID, strings.TrimSpace(r.Header.Get("x-opencode-session")), fp)
	if err != nil {
		apiError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	upID, upKey, err := p.upstreamKey(sess)
	if err != nil {
		apiError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	// ask for token usage on streamed chat completions so tracing/usage is accurate
	outBody := body
	if parsed != nil && stream && strings.HasSuffix(r.URL.Path, "/chat/completions") && p.st.Setting("inject_usage", "1") == "1" {
		so, _ := parsed["stream_options"].(map[string]any)
		if so == nil {
			so = map[string]any{}
		}
		if _, set := so["include_usage"]; !set {
			so["include_usage"] = true
			parsed["stream_options"] = so
			if nb, err := json.Marshal(parsed); err == nil {
				outBody = nb
			}
		}
	}

	rec := reqRecord{
		ts: start.Unix(), session: sess.ID, source: sess.Source, client: clientID, upstream: upID,
		method: r.Method, path: r.URL.Path, model: model, stream: stream, msgCount: fp.Count,
	}
	logBodies := p.st.Setting("log_bodies", "1") == "1"
	if logBodies {
		rec.reqBody = trunc(body, maxLogBody)
	}

	path := strings.TrimPrefix(r.URL.Path, "/v1")
	target := p.upstream + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(outBody))
	if err != nil {
		apiError(w, http.StatusBadGateway, err.Error())
		return
	}
	for k, vs := range r.Header {
		if hopHeaders[http.CanonicalHeaderKey(k)] || strings.EqualFold(k, "x-opencode-session") {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Authorization", "Bearer "+upKey)
	req.Header.Set("x-opencode-session", sess.ID)
	req.ContentLength = int64(len(outBody))

	resp, err := p.client.Do(req)
	if err != nil {
		rec.status, rec.errMsg = http.StatusBadGateway, err.Error()
		rec.latency = time.Since(start)
		p.record(rec, Usage{})
		apiError(w, http.StatusBadGateway, "upstream error: "+err.Error())
		return
	}
	defer resp.Body.Close()
	rec.ttfb = time.Since(start)

	for k, vs := range resp.Header {
		if hopHeaders[http.CanonicalHeaderKey(k)] {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("x-proxy-session", sess.ID)
	w.WriteHeader(resp.StatusCode)

	cap := &capture{headMax: headSize, tailMax: tailSize}
	var tr *tracer
	if stream && resp.StatusCode < 400 {
		tr = &tracer{}
	}
	fl, _ := w.(http.Flusher)
	buf := make([]byte, 32<<10)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			cap.write(buf[:n])
			if tr != nil {
				tr.feed(buf[:n], int(time.Since(start).Milliseconds()))
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				rec.errMsg = "client disconnected: " + werr.Error()
				break
			}
			if fl != nil {
				fl.Flush()
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				rec.errMsg = "upstream read: " + rerr.Error()
			}
			break
		}
	}
	rec.status = resp.StatusCode
	rec.latency = time.Since(start)
	if rec.status >= 400 && rec.errMsg == "" {
		rec.errMsg = trunc(cap.head, 300)
	}
	usage := parseUsage(cap)
	if tr != nil {
		rec.finish, rec.trace = tr.t.Finish, tr.json()
	} else if resp.StatusCode < 400 && len(cap.head) < headSize {
		rec.finish = finishFromBody(cap.head)
	}
	if logBodies {
		if stream {
			rec.respBody = trunc(append(append([]byte{}, cap.head[:min(len(cap.head), maxLogBody/2)]...), cap.tail...), maxLogBody)
		} else {
			rec.respBody = trunc(cap.head, maxLogBody)
		}
	}
	if resp.StatusCode < 300 {
		p.rs.Touch(sess, fp, usage)
	}
	p.record(rec, usage)
}

// upstreamKey returns the key a session is pinned to; if that key was disabled/deleted the session is re-pinned.
func (p *Proxy) upstreamKey(s *Session) (int64, string, error) {
	var key string
	var enabled int
	err := p.st.db.QueryRow(`SELECT key, enabled FROM upstream_keys WHERE id=?`, s.UpstreamKeyID).Scan(&key, &enabled)
	if err == nil && enabled == 1 {
		return s.UpstreamKeyID, key, nil
	}
	nid, perr := p.rs.pickUpstream(s.ClientKeyID)
	if perr != nil {
		return 0, "", perr
	}
	if err := p.st.db.QueryRow(`SELECT key FROM upstream_keys WHERE id=?`, nid).Scan(&key); err != nil {
		return 0, "", errNoUpstream
	}
	p.st.db.Exec(`UPDATE sessions SET upstream_key_id=? WHERE id=?`, nid, s.ID)
	s.UpstreamKeyID = nid
	log.Printf("session %s re-pinned to upstream key %d", s.ID, nid)
	return nid, key, nil
}

type reqRecord struct {
	ts                int64
	session, source   string
	client, upstream  int64
	method, path      string
	model             string
	stream            bool
	status            int
	latency, ttfb     time.Duration
	msgCount          int
	errMsg            string
	finish, trace     string
	reqBody, respBody string
}

func (p *Proxy) record(r reqRecord, u Usage) {
	_, err := p.st.db.Exec(`INSERT INTO requests(ts,session_id,session_source,client_key_id,upstream_key_id,method,path,model,stream,status,
		latency_ms,ttfb_ms,msg_count,prompt_tokens,completion_tokens,cached_tokens,error,req_body,resp_body,finish_reason,trace)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.ts, r.session, r.source, r.client, r.upstream, r.method, r.path, r.model, b2i(r.stream), r.status,
		r.latency.Milliseconds(), r.ttfb.Milliseconds(), r.msgCount, u.Prompt, u.Completion, u.Cached, r.errMsg, r.reqBody, r.respBody, r.finish, r.trace)
	if err != nil {
		log.Printf("record request: %v", err)
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func trunc(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "\n…[truncated]"
}

// capture keeps the beginning and end of a response without buffering all of it.
type capture struct {
	head, tail       []byte
	headMax, tailMax int
}

func (c *capture) write(p []byte) {
	if room := c.headMax - len(c.head); room > 0 {
		n := min(room, len(p))
		c.head = append(c.head, p[:n]...)
	}
	c.tail = append(c.tail, p...)
	if len(c.tail) > 2*c.tailMax {
		c.tail = append([]byte{}, c.tail[len(c.tail)-c.tailMax:]...)
	}
}

type usageJSON struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	InputTokens      int `json:"input_tokens"`
	OutputTokens     int `json:"output_tokens"`
	PromptDetails    struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	InputDetails struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"input_tokens_details"`
}

func (u usageJSON) toUsage() Usage {
	r := Usage{Prompt: u.PromptTokens, Completion: u.CompletionTokens, Cached: u.PromptDetails.CachedTokens}
	if r.Prompt == 0 {
		r.Prompt = u.InputTokens
	}
	if r.Completion == 0 {
		r.Completion = u.OutputTokens
	}
	if r.Cached == 0 {
		r.Cached = u.InputDetails.CachedTokens
	}
	return r
}

type usageEnvelope struct {
	Usage    *usageJSON `json:"usage"`
	Response *struct {
		Usage *usageJSON `json:"usage"`
	} `json:"response"`
}

func (e usageEnvelope) get() (Usage, bool) {
	if e.Usage != nil {
		return e.Usage.toUsage(), true
	}
	if e.Response != nil && e.Response.Usage != nil {
		return e.Response.Usage.toUsage(), true
	}
	return Usage{}, false
}

// parseUsage handles plain JSON bodies (chat + responses) and SSE streams (last event carrying usage).
func parseUsage(c *capture) Usage {
	var env usageEnvelope
	if len(c.head) < headSize && json.Unmarshal(c.head, &env) == nil {
		if u, ok := env.get(); ok {
			return u
		}
	}
	lines := strings.Split(string(c.tail), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(l, "data:") {
			continue
		}
		l = strings.TrimSpace(strings.TrimPrefix(l, "data:"))
		if l == "" || l == "[DONE]" || !strings.Contains(l, "usage") {
			continue
		}
		var e usageEnvelope
		if json.Unmarshal([]byte(l), &e) == nil {
			if u, ok := e.get(); ok {
				return u
			}
		}
	}
	return Usage{}
}
