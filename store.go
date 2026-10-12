package main

import (
	"database/sql"
	"strconv"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS upstream_keys (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	key TEXT NOT NULL,
	enabled INTEGER NOT NULL DEFAULT 1,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS client_keys (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	hash TEXT NOT NULL UNIQUE,
	prefix TEXT NOT NULL,
	enabled INTEGER NOT NULL DEFAULT 1,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
	id TEXT PRIMARY KEY,
	client_key_id INTEGER NOT NULL,
	upstream_key_id INTEGER NOT NULL,
	source TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	last_seen INTEGER NOT NULL,
	expires_at INTEGER NOT NULL,
	expired INTEGER NOT NULL DEFAULT 0,
	requests INTEGER NOT NULL DEFAULT 0,
	prompt_tokens INTEGER NOT NULL DEFAULT 0,
	completion_tokens INTEGER NOT NULL DEFAULT 0,
	cached_tokens INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS sessions_last_seen ON sessions(last_seen);
CREATE TABLE IF NOT EXISTS fingerprints (
	hash TEXT PRIMARY KEY,
	session_id TEXT NOT NULL,
	expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	ts INTEGER NOT NULL,
	session_id TEXT NOT NULL,
	session_source TEXT NOT NULL,
	client_key_id INTEGER NOT NULL,
	upstream_key_id INTEGER NOT NULL,
	method TEXT NOT NULL,
	path TEXT NOT NULL,
	model TEXT NOT NULL DEFAULT '',
	stream INTEGER NOT NULL DEFAULT 0,
	status INTEGER NOT NULL DEFAULT 0,
	latency_ms INTEGER NOT NULL DEFAULT 0,
	ttfb_ms INTEGER NOT NULL DEFAULT 0,
	msg_count INTEGER NOT NULL DEFAULT 0,
	prompt_tokens INTEGER NOT NULL DEFAULT 0,
	completion_tokens INTEGER NOT NULL DEFAULT 0,
	cached_tokens INTEGER NOT NULL DEFAULT 0,
	error TEXT NOT NULL DEFAULT '',
	req_body TEXT NOT NULL DEFAULT '',
	resp_body TEXT NOT NULL DEFAULT '',
	finish_reason TEXT NOT NULL DEFAULT '',
	trace TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS requests_ts ON requests(ts);
CREATE INDEX IF NOT EXISTS requests_session ON requests(session_id, ts);
CREATE TABLE IF NOT EXISTS settings (
	k TEXT PRIMARY KEY,
	v TEXT NOT NULL
);
`

type Store struct {
	db *sql.DB

	mu sync.RWMutex
	// fp is an in-memory index of fingerprint hash -> session id for fast prefix lookups.
	fp map[string]fpEntry
}

type fpEntry struct {
	session string
	expires int64
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite: serialize writes, avoids SQLITE_BUSY
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	for _, col := range []string{"finish_reason", "trace"} {
		var n int
		db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('requests') WHERE name=?`, col).Scan(&n)
		if n == 0 {
			if _, err := db.Exec(`ALTER TABLE requests ADD COLUMN ` + col + ` TEXT NOT NULL DEFAULT ''`); err != nil {
				return nil, err
			}
		}
	}
	s := &Store{db: db, fp: map[string]fpEntry{}}
	rows, err := db.Query(`SELECT hash, session_id, expires_at FROM fingerprints WHERE expires_at > ?`, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var h, sid string
		var exp int64
		if err := rows.Scan(&h, &sid, &exp); err != nil {
			return nil, err
		}
		s.fp[h] = fpEntry{sid, exp}
	}
	return s, rows.Err()
}

func (s *Store) Close() error { return s.db.Close() }

// ---- settings ----

func (s *Store) Setting(key, def string) string {
	var v string
	if err := s.db.QueryRow(`SELECT v FROM settings WHERE k=?`, key).Scan(&v); err != nil {
		return def
	}
	return v
}

func (s *Store) SetSetting(key, val string) error {
	_, err := s.db.Exec(`INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, key, val)
	return err
}

func (s *Store) SettingInt(key string, def int) int {
	n, err := strconv.Atoi(s.Setting(key, ""))
	if err != nil {
		return def
	}
	return n
}

func (s *Store) SessionTTL() time.Duration {
	h := s.SettingInt("session_ttl_hours", 24)
	if h < 1 {
		h = 1
	}
	return time.Duration(h) * time.Hour
}

// ---- fingerprints ----

func (s *Store) LookupFingerprint(hash string) (string, bool) {
	s.mu.RLock()
	e, ok := s.fp[hash]
	s.mu.RUnlock()
	if !ok || e.expires <= time.Now().Unix() {
		return "", false
	}
	return e.session, true
}

func (s *Store) PutFingerprints(session string, hashes []string, expires int64) {
	s.mu.Lock()
	for _, h := range hashes {
		s.fp[h] = fpEntry{session, expires}
	}
	s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return
	}
	for _, h := range hashes {
		tx.Exec(`INSERT INTO fingerprints(hash,session_id,expires_at) VALUES(?,?,?)
			ON CONFLICT(hash) DO UPDATE SET session_id=excluded.session_id, expires_at=excluded.expires_at`, h, session, expires)
	}
	tx.Commit()
}

func (s *Store) DropSessionFingerprints(session string) {
	s.mu.Lock()
	for h, e := range s.fp {
		if e.session == session {
			delete(s.fp, h)
		}
	}
	s.mu.Unlock()
	s.db.Exec(`DELETE FROM fingerprints WHERE session_id=?`, session)
}

// ExpireSessions marks sessions past their expiry as expired and drops their fingerprints.
// Request history is kept (see PruneRequests).
func (s *Store) ExpireSessions() {
	now := time.Now().Unix()
	s.db.Exec(`UPDATE sessions SET expired=1 WHERE expired=0 AND expires_at<=?`, now)
	s.db.Exec(`DELETE FROM fingerprints WHERE expires_at<=?`, now)
	s.mu.Lock()
	for h, e := range s.fp {
		if e.expires <= now {
			delete(s.fp, h)
		}
	}
	s.mu.Unlock()
}

func (s *Store) PruneRequests(days int) {
	if days <= 0 {
		return
	}
	cut := time.Now().Add(-time.Duration(days) * 24 * time.Hour).Unix()
	s.db.Exec(`DELETE FROM requests WHERE ts<?`, cut)
	s.db.Exec(`DELETE FROM sessions WHERE expired=1 AND last_seen<?`, cut)
}
