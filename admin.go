package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Admin struct {
	st       *Store
	password string
	secret   []byte
	catalog  *ModelCatalog // set by main; enables /admin/api/models
	inflight *Inflight     // set by main; enables /admin/api/inflight
}

func NewAdmin(st *Store, password string) *Admin {
	sec := st.Setting("admin_secret", "")
	if sec == "" {
		b := make([]byte, 32)
		randRead(b)
		sec = hex.EncodeToString(b)
		st.SetSetting("admin_secret", sec)
	}
	return &Admin{st: st, password: password, secret: []byte(sec)}
}

const cookieName = "ocs_admin"

func (a *Admin) sign(exp int64) string {
	m := hmac.New(sha256.New, a.secret)
	fmt.Fprintf(m, "%d", exp)
	return fmt.Sprintf("%d.%s", exp, hex.EncodeToString(m.Sum(nil)))
}

func (a *Admin) valid(tok string) bool {
	i := strings.IndexByte(tok, '.')
	if i < 0 {
		return false
	}
	exp, err := strconv.ParseInt(tok[:i], 10, 64)
	if err != nil || exp < time.Now().Unix() {
		return false
	}
	return hmac.Equal([]byte(tok), []byte(a.sign(exp)))
}

func (a *Admin) authed(r *http.Request) bool {
	c, err := r.Cookie(cookieName)
	return err == nil && a.valid(c.Value)
}

func (a *Admin) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.authed(r) {
			writeJSON(w, 401, map[string]any{"error": "unauthorized"})
			return
		}
		// CSRF: cookies are SameSite=Strict, and mutations must also carry a header a cross-site form cannot set
		if r.Method != http.MethodGet && r.Header.Get("X-Admin") != "1" {
			writeJSON(w, 403, map[string]any{"error": "missing X-Admin header"})
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func decode(r *http.Request, v any) error {
	return json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20)).Decode(v)
}

func fail(w http.ResponseWriter, err error) {
	writeJSON(w, 500, map[string]any{"error": err.Error()})
}

func (a *Admin) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/login", a.loginPage)
	mux.HandleFunc("POST /admin/login", a.loginForm)
	mux.HandleFunc("POST /admin/api/login", a.login)
	mux.HandleFunc("POST /admin/api/logout", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: cookieName, Path: "/admin", MaxAge: -1})
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	g := a.guard
	mux.HandleFunc("GET /admin/api/me", g(func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]any{"ok": true}) }))
	mux.HandleFunc("GET /admin/api/overview", g(a.overview))
	mux.HandleFunc("GET /admin/api/upstream-keys", g(a.listUpstream))
	mux.HandleFunc("POST /admin/api/upstream-keys", g(a.addUpstream))
	mux.HandleFunc("PATCH /admin/api/upstream-keys/{id}", g(a.patchUpstream))
	mux.HandleFunc("DELETE /admin/api/upstream-keys/{id}", g(a.deleteRow("upstream_keys")))
	mux.HandleFunc("GET /admin/api/client-keys", g(a.listClient))
	mux.HandleFunc("POST /admin/api/client-keys", g(a.addClient))
	mux.HandleFunc("PATCH /admin/api/client-keys/{id}", g(a.patchClient))
	mux.HandleFunc("DELETE /admin/api/client-keys/{id}", g(a.deleteRow("client_keys")))
	mux.HandleFunc("GET /admin/api/sessions", g(a.listSessions))
	mux.HandleFunc("GET /admin/api/sessions/{id}", g(a.getSession))
	mux.HandleFunc("POST /admin/api/sessions/{id}/expire", g(a.expireSession))
	mux.HandleFunc("GET /admin/api/requests", g(a.listRequests))
	mux.HandleFunc("GET /admin/api/requests/{id}", g(a.getRequest))
	mux.HandleFunc("GET /admin/api/inflight", g(func(w http.ResponseWriter, r *http.Request) {
		if a.inflight == nil {
			writeJSON(w, 200, []any{})
			return
		}
		writeJSON(w, 200, a.inflight.Snapshot())
	}))
	mux.HandleFunc("GET /admin/api/models", g(a.listModels))
	mux.HandleFunc("GET /admin/api/settings", g(a.getSettings))
	mux.HandleFunc("PUT /admin/api/settings", g(a.putSettings))
}

// checkPassword verifies pw and, on success, sets the session cookie. On failure it sleeps to slow brute force.
func (a *Admin) checkPassword(w http.ResponseWriter, r *http.Request, pw string) bool {
	if subtle.ConstantTimeCompare([]byte(pw), []byte(a.password)) != 1 {
		time.Sleep(700 * time.Millisecond)
		return false
	}
	exp := time.Now().Add(12 * time.Hour)
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: a.sign(exp.Unix()), Path: "/admin", Expires: exp,
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"})
	return true
}

func (a *Admin) login(w http.ResponseWriter, r *http.Request) {
	var in struct{ Password string }
	if decode(r, &in) != nil || !a.checkPassword(w, r, in.Password) {
		writeJSON(w, 401, map[string]any{"error": "wrong password"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

// loginPage serves the static sign-in form (plain HTML, no JS).
func (a *Admin) loginPage(w http.ResponseWriter, r *http.Request) {
	if a.authed(r) {
		http.Redirect(w, r, "/admin", http.StatusFound)
		return
	}
	page := loginHTML
	if r.URL.Query().Get("err") != "" {
		page = bytes.Replace(page, []byte("<!--ERR-->"), []byte(`<div class="err">Wrong password</div>`), 1)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(page)
}

func (a *Admin) loginForm(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	if r.ParseForm() != nil || !a.checkPassword(w, r, r.PostForm.Get("password")) {
		http.Redirect(w, r, "/admin/login?err=1", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/admin", http.StatusSeeOther)
}

// query runs SQL and returns rows as maps (column -> value) for direct JSON output.
func (a *Admin) query(q string, args ...any) ([]map[string]any, error) {
	rows, err := a.st.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := []map[string]any{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := map[string]any{}
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				m[c] = string(b)
			} else {
				m[c] = vals[i]
			}
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (a *Admin) one(q string, args ...any) map[string]any {
	r, err := a.query(q, args...)
	if err != nil || len(r) == 0 {
		return map[string]any{}
	}
	return r[0]
}

func intParam(r *http.Request, name string, def int) int {
	if n, err := strconv.Atoi(r.URL.Query().Get(name)); err == nil {
		return n
	}
	return def
}

func (a *Admin) overview(w http.ResponseWriter, r *http.Request) {
	hours := intParam(r, "hours", 24)
	since := time.Now().Add(-time.Duration(hours) * time.Hour).Unix()
	bucket := int64(3600)
	switch {
	case hours <= 1:
		bucket = 60
	case hours <= 6:
		bucket = 300
	case hours > 72:
		bucket = 86400
	}
	// optional per-key filters apply to every panel
	cond, args := "ts>=?", []any{since}
	for param, col := range map[string]string{"client": "client_key_id", "upstream": "upstream_key_id"} {
		if v := r.URL.Query().Get(param); v != "" {
			cond += " AND " + col + "=?"
			args = append(args, v)
		}
	}
	q := func(sel, tail string, extra ...any) []map[string]any {
		rows, _ := a.query("SELECT "+sel+" FROM requests WHERE "+cond+" "+tail, append(append([]any{}, args...), extra...)...)
		return rows
	}
	first := func(sel string) map[string]any {
		if rows := q(sel, ""); len(rows) > 0 {
			return rows[0]
		}
		return map[string]any{}
	}
	tokens := `COALESCE(SUM(prompt_tokens),0) prompt_tokens, COALESCE(SUM(completion_tokens),0) completion_tokens, COALESCE(SUM(cached_tokens),0) cached_tokens, COALESCE(SUM(status>=400),0) errors`
	out := map[string]any{
		"totals": first(`COUNT(*) requests, ` + tokens + `, COALESCE(AVG(latency_ms),0) avg_latency_ms, COALESCE(AVG(CASE WHEN stream=1 THEN ttfb_ms END),0) avg_ttfb_ms,
			COALESCE(SUM(stream),0) streams, COUNT(DISTINCT session_id) sessions`),
		"active_sessions": a.one(`SELECT COUNT(*) n FROM sessions WHERE expired=0 AND expires_at>?`, time.Now().Unix())["n"],
		"bucket":          bucket,
		"filters": map[string]any{
			"upstream": a.rowsOrEmpty(`SELECT id, name FROM upstream_keys ORDER BY id`),
			"client":   a.rowsOrEmpty(`SELECT id, name FROM client_keys ORDER BY id`),
		},
	}
	out["series"] = q(`(ts/`+strconv.FormatInt(bucket, 10)+`)*`+strconv.FormatInt(bucket, 10)+` t, COUNT(*) requests, `+tokens+`,
		COALESCE(AVG(latency_ms),0) avg_latency_ms, COALESCE(AVG(CASE WHEN stream=1 THEN ttfb_ms END),0) avg_ttfb_ms`, "GROUP BY t ORDER BY t")
	out["by_upstream"] = q(`upstream_key_id id, COALESCE((SELECT name FROM upstream_keys WHERE id=upstream_key_id),'(deleted)') name, COUNT(*) requests, `+tokens, "GROUP BY upstream_key_id ORDER BY requests DESC")
	out["by_client"] = q(`client_key_id id, COALESCE((SELECT name FROM client_keys WHERE id=client_key_id),'(deleted)') name, COUNT(*) requests, `+tokens, "GROUP BY client_key_id ORDER BY requests DESC")
	out["by_model"] = q(`model, COUNT(*) requests, `+tokens+`, COALESCE(AVG(latency_ms),0) avg_latency_ms`, "GROUP BY model ORDER BY requests DESC LIMIT 20")
	out["by_source"] = q(`session_source source, COUNT(*) requests`, "GROUP BY session_source")
	out["by_finish"] = q(`CASE WHEN status>=400 THEN 'http error' WHEN finish_reason='' AND stream=1 THEN 'no finish (aborted?)' WHEN finish_reason='' THEN 'unknown' ELSE finish_reason END reason, COUNT(*) requests`, "GROUP BY reason ORDER BY requests DESC")
	out["by_status"] = q(`status, COUNT(*) requests`, "GROUP BY status ORDER BY requests DESC")
	// latency percentiles (overall, computed here since SQLite has no percentile function)
	lat := q(`latency_ms, ttfb_ms, stream`, "")
	var tot, ttfb []int
	for _, m := range lat {
		tot = append(tot, int(m["latency_ms"].(int64)))
		if m["stream"].(int64) == 1 {
			ttfb = append(ttfb, int(m["ttfb_ms"].(int64)))
		}
	}
	out["percentiles"] = map[string]any{
		"latency_p50": pct(tot, 50), "latency_p95": pct(tot, 95), "ttfb_p50": pct(ttfb, 50), "ttfb_p95": pct(ttfb, 95),
	}
	writeJSON(w, 200, out)
}

func (a *Admin) rowsOrEmpty(q string) []map[string]any {
	rows, err := a.query(q)
	if err != nil {
		return []map[string]any{}
	}
	return rows
}

func pct(v []int, p int) int {
	if len(v) == 0 {
		return 0
	}
	sort.Ints(v)
	i := (len(v)*p + 99) / 100
	if i > 0 {
		i--
	}
	return v[min(i, len(v)-1)]
}

// ---- upstream keys ----

func mask(k string) string {
	if len(k) <= 10 {
		return strings.Repeat("•", len(k))
	}
	return k[:4] + "…" + k[len(k)-4:]
}

func (a *Admin) listUpstream(w http.ResponseWriter, r *http.Request) {
	rows, err := a.query(`SELECT k.id, k.name, k.key, k.enabled, k.created_at,
		(SELECT COUNT(*) FROM sessions s WHERE s.upstream_key_id=k.id AND s.expired=0 AND s.expires_at>?) active_sessions,
		(SELECT COUNT(*) FROM requests q WHERE q.upstream_key_id=k.id) requests,
		(SELECT COALESCE(SUM(prompt_tokens+completion_tokens),0) FROM requests q WHERE q.upstream_key_id=k.id) tokens,
		(SELECT COALESCE(SUM(status>=400),0) FROM requests q WHERE q.upstream_key_id=k.id AND q.ts>?) errors_24h,
		(SELECT MAX(ts) FROM requests q WHERE q.upstream_key_id=k.id) last_used
		FROM upstream_keys k ORDER BY k.id`, time.Now().Unix(), time.Now().Add(-24*time.Hour).Unix())
	if err != nil {
		fail(w, err)
		return
	}
	for _, m := range rows {
		m["key"] = mask(m["key"].(string))
	}
	writeJSON(w, 200, rows)
}

func (a *Admin) addUpstream(w http.ResponseWriter, r *http.Request) {
	var in struct{ Name, Key string }
	if decode(r, &in) != nil || strings.TrimSpace(in.Key) == "" {
		writeJSON(w, 400, map[string]any{"error": "key required"})
		return
	}
	if in.Name == "" {
		in.Name = "key-" + mask(in.Key)
	}
	res, err := a.st.db.Exec(`INSERT INTO upstream_keys(name,key,enabled,created_at) VALUES(?,?,1,?)`, in.Name, strings.TrimSpace(in.Key), time.Now().Unix())
	if err != nil {
		fail(w, err)
		return
	}
	id, _ := res.LastInsertId()
	writeJSON(w, 200, map[string]any{"id": id})
}

func (a *Admin) patchEnabledName(table string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Name    *string
			Enabled *bool
		}
		if decode(r, &in) != nil {
			writeJSON(w, 400, map[string]any{"error": "bad json"})
			return
		}
		id := r.PathValue("id")
		if in.Name != nil {
			a.st.db.Exec(`UPDATE `+table+` SET name=? WHERE id=?`, *in.Name, id)
		}
		if in.Enabled != nil {
			a.st.db.Exec(`UPDATE `+table+` SET enabled=? WHERE id=?`, b2i(*in.Enabled), id)
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	}
}

func (a *Admin) patchUpstream(w http.ResponseWriter, r *http.Request) {
	a.patchEnabledName("upstream_keys")(w, r)
}
func (a *Admin) patchClient(w http.ResponseWriter, r *http.Request) {
	a.patchEnabledName("client_keys")(w, r)
}

func (a *Admin) deleteRow(table string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, err := a.st.db.Exec(`DELETE FROM `+table+` WHERE id=?`, r.PathValue("id")); err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	}
}

// ---- client keys ----

func (a *Admin) listClient(w http.ResponseWriter, r *http.Request) {
	rows, err := a.query(`SELECT c.id, c.name, c.prefix, c.enabled, c.created_at,
		(SELECT COUNT(*) FROM requests q WHERE q.client_key_id=c.id) requests,
		(SELECT COALESCE(SUM(prompt_tokens+completion_tokens),0) FROM requests q WHERE q.client_key_id=c.id) tokens,
		(SELECT MAX(ts) FROM requests q WHERE q.client_key_id=c.id) last_used
		FROM client_keys c ORDER BY c.id`)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, rows)
}

func (a *Admin) addClient(w http.ResponseWriter, r *http.Request) {
	var in struct{ Name string }
	decode(r, &in)
	if in.Name == "" {
		in.Name = "client"
	}
	b := make([]byte, 24)
	randRead(b)
	key := "ocp_" + hex.EncodeToString(b)
	res, err := a.st.db.Exec(`INSERT INTO client_keys(name,hash,prefix,enabled,created_at) VALUES(?,?,?,1,?)`,
		in.Name, hashKey(key), key[:8], time.Now().Unix())
	if err != nil {
		fail(w, err)
		return
	}
	id, _ := res.LastInsertId()
	writeJSON(w, 200, map[string]any{"id": id, "key": key}) // shown once
}

// ---- sessions ----

func (a *Admin) listSessions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	where, args := []string{"1=1"}, []any{}
	if s := q.Get("q"); s != "" {
		where = append(where, "s.id LIKE ?")
		args = append(args, "%"+s+"%")
	}
	switch q.Get("state") {
	case "active":
		where = append(where, "s.expired=0 AND s.expires_at>?")
		args = append(args, time.Now().Unix())
	case "expired":
		where = append(where, "(s.expired=1 OR s.expires_at<=?)")
		args = append(args, time.Now().Unix())
	}
	if v := q.Get("upstream"); v != "" {
		where = append(where, "s.upstream_key_id=?")
		args = append(args, v)
	}
	if v := q.Get("client"); v != "" {
		where = append(where, "s.client_key_id=?")
		args = append(args, v)
	}
	args = append(args, intParam(r, "limit", 100), intParam(r, "offset", 0))
	rows, err := a.query(`SELECT s.id, s.client_key_id, s.upstream_key_id, s.source, s.created_at, s.last_seen, s.expires_at, s.expired,
		s.requests, s.prompt_tokens, s.completion_tokens, s.cached_tokens,
		COALESCE((SELECT name FROM upstream_keys WHERE id=s.upstream_key_id),'(deleted)') upstream_name,
		COALESCE((SELECT name FROM client_keys WHERE id=s.client_key_id),'(deleted)') client_name
		FROM sessions s WHERE `+strings.Join(where, " AND ")+` ORDER BY s.last_seen DESC LIMIT ? OFFSET ?`, args...)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, rows)
}

func (a *Admin) getSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s := a.one(`SELECT * FROM sessions WHERE id=?`, id)
	if len(s) == 0 {
		writeJSON(w, 404, map[string]any{"error": "not found"})
		return
	}
	reqs, _ := a.query(`SELECT id, ts, method, path, model, stream, status, latency_ms, ttfb_ms, msg_count, prompt_tokens, completion_tokens,
		cached_tokens, session_source, error, finish_reason, attempts, attempt_log FROM requests WHERE session_id=? ORDER BY id DESC LIMIT 500`, id)
	writeJSON(w, 200, map[string]any{"session": s, "requests": reqs})
}

func (a *Admin) expireSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a.st.db.Exec(`UPDATE sessions SET expired=1, expires_at=? WHERE id=?`, time.Now().Unix(), id)
	a.st.DropSessionFingerprints(id)
	writeJSON(w, 200, map[string]any{"ok": true})
}

// ---- requests ----

func (a *Admin) listRequests(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	where, args := []string{"1=1"}, []any{}
	add := func(col, param string) {
		if v := q.Get(param); v != "" {
			where = append(where, col+"=?")
			args = append(args, v)
		}
	}
	add("session_id", "session")
	add("model", "model")
	add("upstream_key_id", "upstream")
	add("client_key_id", "client")
	add("session_source", "source")
	add("finish_reason", "finish")
	switch q.Get("status") {
	case "error":
		where = append(where, "status>=400")
	case "ok":
		where = append(where, "status<400")
	}
	if v := q.Get("before"); v != "" {
		where = append(where, "id<?")
		args = append(args, v)
	}
	args = append(args, intParam(r, "limit", 100))
	rows, err := a.query(`SELECT id, ts, session_id, session_source, client_key_id, upstream_key_id, method, path, model, stream, status,
		latency_ms, ttfb_ms, msg_count, prompt_tokens, completion_tokens, cached_tokens, error, finish_reason, attempts, attempt_log
		FROM requests WHERE `+strings.Join(where, " AND ")+` ORDER BY id DESC LIMIT ?`, args...)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, rows)
}

func (a *Admin) getRequest(w http.ResponseWriter, r *http.Request) {
	m := a.one(`SELECT * FROM requests WHERE id=?`, r.PathValue("id"))
	if len(m) == 0 {
		writeJSON(w, 404, map[string]any{"error": "not found"})
		return
	}
	writeJSON(w, 200, m)
}

// ---- settings ----

var settingDefaults = map[string]string{
	"session_ttl_hours":      "24",
	"request_retention_days": "30",
	"log_bodies":             "1",
	"inject_usage":           "1",
	"log_requests":           "1",
	"ignore_client_session":  "0",
	"retry_max":              "2",
	"retry_backoff_ms":       "500",
	"model_map":              "",
	"fallback_model":         "",
}

// textSettings are free-form; retry_max may be 0 (retries off).
var textSettings = map[string]bool{"model_map": true, "fallback_model": true}

var boolSettings = map[string]bool{"log_bodies": true, "inject_usage": true, "log_requests": true, "ignore_client_session": true}

func (a *Admin) getSettings(w http.ResponseWriter, r *http.Request) {
	out := map[string]string{}
	for k, d := range settingDefaults {
		out[k] = a.st.Setting(k, d)
	}
	writeJSON(w, 200, out)
}

func (a *Admin) putSettings(w http.ResponseWriter, r *http.Request) {
	var in map[string]string
	if decode(r, &in) != nil {
		writeJSON(w, 400, map[string]any{"error": "bad json"})
		return
	}
	for k, v := range in {
		if _, ok := settingDefaults[k]; !ok {
			continue
		}
		switch {
		case k == "model_map":
			if _, err := parseModelMap(v); err != nil {
				writeJSON(w, 400, map[string]any{"error": "model_map: " + err.Error()})
				return
			}
		case textSettings[k], boolSettings[k]:
		case k == "retry_max":
			if n, err := strconv.Atoi(v); err != nil || n < 0 || n > 10 {
				writeJSON(w, 400, map[string]any{"error": "retry_max must be an integer between 0 and 10"})
				return
			}
		default:
			if n, err := strconv.Atoi(v); err != nil || n < 1 {
				writeJSON(w, 400, map[string]any{"error": k + " must be a positive integer"})
				return
			}
		}
		if err := a.st.SetSetting(k, v); err != nil {
			fail(w, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

var _ = sql.ErrNoRows

// listModels returns the upstream model list (plus aliases); ?refresh=1 bypasses the cache.
func (a *Admin) listModels(w http.ResponseWriter, r *http.Request) {
	if a.catalog == nil {
		writeJSON(w, 503, map[string]any{"error": "model catalog unavailable"})
		return
	}
	list, err := a.catalog.List(r.Context(), r.URL.Query().Get("refresh") == "1")
	out := map[string]any{"data": list}
	if err != nil {
		out["error"] = err.Error()
	}
	writeJSON(w, 200, out)
}
