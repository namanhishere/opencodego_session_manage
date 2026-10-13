package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---- model mapping ----

// parseModelMap parses lines of the form "alias = target1, target2". The first target is used; the
// following ones are fallbacks tried in order when the previous one fails. '#' starts a comment.
func parseModelMap(s string) (map[string][]string, error) {
	out := map[string][]string{}
	for i, line := range strings.Split(s, "\n") {
		if j := strings.IndexByte(line, '#'); j >= 0 {
			line = line[:j]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("line %d: expected \"model = target[, fallback...]\"", i+1)
		}
		var targets []string
		for _, t := range strings.Split(v, ",") {
			if t = strings.TrimSpace(t); t != "" {
				targets = append(targets, t)
			}
		}
		if len(targets) == 0 {
			return nil, fmt.Errorf("line %d: no target model for %q", i+1, k)
		}
		out[k] = targets
	}
	return out, nil
}

// modelTargets lists the models to try for a requested model, in order: the mapped targets (or the
// model itself), followed by the global fallback model.
func modelTargets(st *Store, model string) []string {
	m, _ := parseModelMap(st.Setting("model_map", ""))
	targets := []string{model}
	if t, ok := m[model]; ok {
		targets = t
	}
	if fb := strings.TrimSpace(st.Setting("fallback_model", "")); fb != "" {
		targets = append(append([]string{}, targets...), fb)
	}
	seen := map[string]bool{}
	out := targets[:0:0]
	for _, t := range targets {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// ---- model catalog (fetched from upstream) ----

const (
	modelsTTL        = 5 * time.Minute
	modelsFailBackof = 30 * time.Second
)

type ModelCatalog struct {
	st       *Store
	upstream string
	client   *http.Client

	mu      sync.Mutex
	raw     []map[string]any // upstream "data" entries
	ids     map[string]bool
	fetched time.Time
	failed  time.Time
}

func NewModelCatalog(st *Store, upstream string, client *http.Client) *ModelCatalog {
	return &ModelCatalog{st: st, upstream: upstream, client: client}
}

func (c *ModelCatalog) fetch(ctx context.Context) error {
	var key string
	if err := c.st.db.QueryRow(`SELECT key FROM upstream_keys WHERE enabled=1 ORDER BY id LIMIT 1`).Scan(&key); err != nil {
		return errNoUpstream
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.upstream+"/models", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("upstream /models returned %d: %s", resp.StatusCode, trunc(b, 200))
	}
	var env struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return fmt.Errorf("upstream /models: %w", err)
	}
	ids := map[string]bool{}
	for _, m := range env.Data {
		if id, _ := m["id"].(string); id != "" {
			ids[id] = true
		}
	}
	c.raw, c.ids, c.fetched = env.Data, ids, time.Now()
	return nil
}

// Get returns the upstream model list, refetching when older than the TTL (or when force is set).
// If a refetch fails the previous list is kept and the error is returned alongside it.
func (c *ModelCatalog) Get(ctx context.Context, force bool) ([]map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !force && c.raw != nil && time.Since(c.fetched) < modelsTTL {
		return c.raw, nil
	}
	if !force && time.Since(c.failed) < modelsFailBackof {
		return c.raw, nil
	}
	if err := c.fetch(ctx); err != nil {
		c.failed = time.Now()
		return c.raw, err
	}
	c.failed = time.Time{}
	return c.raw, nil
}

// Known reports whether the model is listed upstream. known=false means the list is unavailable and
// nothing can be said about the model.
func (c *ModelCatalog) Known(ctx context.Context, model string) (listed, known bool) {
	if _, err := c.Get(ctx, false); c.ids == nil && err != nil {
		return false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil {
		return false, false
	}
	return c.ids[model], true
}

// List is the upstream list plus an entry for every alias in the model map.
func (c *ModelCatalog) List(ctx context.Context, force bool) ([]map[string]any, error) {
	raw, err := c.Get(ctx, force)
	out := append([]map[string]any{}, raw...)
	have := map[string]bool{}
	for _, m := range out {
		id, _ := m["id"].(string)
		have[id] = true
	}
	mm, _ := parseModelMap(c.st.Setting("model_map", ""))
	aliases := make([]string, 0, len(mm))
	for a := range mm {
		if !have[a] {
			aliases = append(aliases, a)
		}
	}
	sort.Strings(aliases)
	for _, a := range aliases {
		out = append(out, map[string]any{"id": a, "object": "model", "created": 0, "owned_by": "proxy-alias", "maps_to": mm[a]})
	}
	return out, err
}

func (p *Proxy) serveModels(w http.ResponseWriter, r *http.Request) {
	list, err := p.catalog.List(r.Context(), false)
	if err != nil && len(list) == 0 {
		status := http.StatusBadGateway
		if err == errNoUpstream {
			status = http.StatusServiceUnavailable
		}
		apiError(w, status, "fetch models: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.Header().Set("x-proxy-models-stale", "1")
	}
	json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": list})
}

// ---- retry ----

var retryStatus = map[int]bool{408: true, 425: true, 429: true, 500: true, 502: true, 503: true, 504: true, 529: true}

const (
	maxRetryWait      = 30 * time.Second // cap on exponential backoff
	maxRetryAfterWait = 10 * time.Second // a longer Retry-After means quota/outage: report it instead of waiting
)

func retryWait(st *Store, attempt int, resp *http.Response) (time.Duration, bool) {
	base := time.Duration(st.SettingInt("retry_backoff_ms", 500)) * time.Millisecond
	d := base << attempt
	if resp != nil {
		if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
			if n, err := strconv.Atoi(ra); err == nil {
				d = time.Duration(n) * time.Second
			} else if t, err := http.ParseTime(ra); err == nil {
				d = time.Until(t)
			}
			if d > maxRetryAfterWait {
				return 0, false // asked to wait too long (e.g. quota reset): give up and report the error
			}
		}
	}
	if d > maxRetryWait {
		d = maxRetryWait
	}
	if d < 0 {
		d = 0
	}
	return d, true
}

// modelRejected reports whether an error response means the model itself is unusable, in which case
// the next model in the chain is tried.
func modelRejected(status int, body []byte) bool {
	switch status {
	case 404:
		return true
	case 400, 422:
		s := strings.ToLower(string(body))
		return strings.Contains(s, "model") && (strings.Contains(s, "not found") || strings.Contains(s, "not support") ||
			strings.Contains(s, "does not exist") || strings.Contains(s, "unknown") || strings.Contains(s, "invalid") ||
			strings.Contains(s, "unavailable") || strings.Contains(s, "not available"))
	}
	return false
}

// sleepCtx waits for d or until ctx is done; it reports whether the full wait elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
