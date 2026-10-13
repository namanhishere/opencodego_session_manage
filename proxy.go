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
	"strconv"
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
	catalog  *ModelCatalog
}

func NewProxy(st *Store, rs *Resolver, upstream string) *Proxy {
	p := &Proxy{
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
	p.catalog = NewModelCatalog(st, p.upstream, p.client)
	return p
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
	if r.Method == http.MethodGet && strings.TrimSuffix(r.URL.Path, "/") == "/v1/models" {
		p.serveModels(w, r)
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

	clientSession := strings.TrimSpace(r.Header.Get("x-opencode-session"))
	if p.st.Setting("ignore_client_session", "0") == "1" {
		clientSession = "" // always derive the session from the message chain
	}
	sess, err := p.rs.Resolve(clientID, clientSession, fp)
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
	injected := false
	if parsed != nil && stream && strings.HasSuffix(r.URL.Path, "/chat/completions") && p.st.Setting("inject_usage", "1") == "1" {
		so, _ := parsed["stream_options"].(map[string]any)
		if so == nil {
			so = map[string]any{}
		}
		if _, set := so["include_usage"]; !set {
			so["include_usage"] = true
			parsed["stream_options"] = so
			injected = true
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

	resp, usedModel, attempts, err := p.forward(r, target, body, parsed, injected, model, sess.ID, upKey)
	w.Header().Set("x-proxy-attempts", strconv.Itoa(attempts))
	if usedModel != "" && usedModel != model {
		rec.model = usedModel
		w.Header().Set("x-proxy-model", usedModel)
	}
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
	if p.st.Setting("log_requests", "1") != "1" {
		return
	}
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

// forward sends the request upstream, retrying transient failures (network errors, 408/429/5xx) with
// exponential backoff on the same key, and walking the model chain (see modelTargets) when a model is
// rejected or keeps failing. Retrying is only possible until the first response byte reaches the
// client, which is always the case here because the response is only returned once its status is final.
// It returns the final response (error bodies are buffered so they can still be inspected), the model
// that produced it and the number of upstream attempts made.
func (p *Proxy) forward(r *http.Request, target string, body []byte, parsed map[string]any, dirty bool, model, sessionID, upKey string) (*http.Response, string, int, error) {
	ctx := r.Context()
	targets := []string{model}
	if parsed != nil && model != "" {
		targets = modelTargets(p.st, model)
		if len(targets) > 1 {
			// skip targets the upstream does not list, as long as one remains
			var ok []string
			for _, t := range targets {
				if listed, known := p.catalog.Known(ctx, t); !known || listed {
					ok = append(ok, t)
				}
			}
			if len(ok) > 0 {
				targets = ok
			}
		}
	}
	maxRetries := p.st.SettingInt("retry_max", 2)
	attempts := 0
	var lastResp *http.Response
	var lastErr error
	var lastModel string
	for ti, tm := range targets {
		out := body
		if parsed != nil && (dirty || tm != model) {
			if tm != model {
				parsed["model"] = tm
			}
			nb, err := json.Marshal(parsed)
			if err != nil {
				return nil, tm, attempts, err
			}
			out = nb
		}
		lastModel = tm
		last := ti == len(targets)-1
		for attempt := 0; ; attempt++ {
			attempts++
			req, err := http.NewRequestWithContext(ctx, r.Method, target, bytes.NewReader(out))
			if err != nil {
				return nil, tm, attempts, err
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
			req.Header.Set("x-opencode-session", sessionID)
			req.ContentLength = int64(len(out))

			resp, err := p.client.Do(req)
			var errBody []byte
			if err == nil && resp.StatusCode >= 400 {
				errBody, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
				resp.Body.Close()
				resp.Body = io.NopCloser(bytes.NewReader(errBody))
			}
			if err == nil && resp.StatusCode < 400 {
				return resp, tm, attempts, nil
			}
			if ctx.Err() != nil { // client went away: stop immediately
				if err == nil {
					resp.Body.Close()
				}
				return nil, tm, attempts, ctx.Err()
			}
			lastErr = err
			if lastResp != nil {
				lastResp.Body.Close()
			}
			lastResp = resp
			if err == nil && !retryStatus[resp.StatusCode] {
				if !last && modelRejected(resp.StatusCode, errBody) {
					log.Printf("session %s: model %q rejected (%d), trying next", sessionID, tm, resp.StatusCode)
					break
				}
				return resp, tm, attempts, nil // permanent client error: report as is
			}
			if attempt < maxRetries {
				if wait, ok := retryWait(p.st, attempt, resp); ok {
					if err != nil {
						log.Printf("session %s: upstream error %v, retry %d/%d in %s", sessionID, err, attempt+1, maxRetries, wait)
					} else {
						log.Printf("session %s: upstream %d, retry %d/%d in %s", sessionID, resp.StatusCode, attempt+1, maxRetries, wait)
					}
					if resp != nil {
						resp.Body.Close()
						lastResp = nil
					}
					if !sleepCtx(ctx, wait) {
						return nil, tm, attempts, ctx.Err()
					}
					continue
				}
			}
			if !last {
				log.Printf("session %s: model %q kept failing, trying next", sessionID, tm)
			}
			break
		}
	}
	if lastResp != nil {
		return lastResp, lastModel, attempts, nil
	}
	return nil, lastModel, attempts, lastErr
}
