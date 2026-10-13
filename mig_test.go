package main

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func TestMigrateOldRequestsTable(t *testing.T) {
	p := filepath.Join(t.TempDir(), "old.db")
	db, _ := sql.Open("sqlite", p)
	db.Exec(`CREATE TABLE requests (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, session_id TEXT NOT NULL, session_source TEXT NOT NULL,
		client_key_id INTEGER NOT NULL, upstream_key_id INTEGER NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL, model TEXT NOT NULL DEFAULT '',
		stream INTEGER NOT NULL DEFAULT 0, status INTEGER NOT NULL DEFAULT 0, latency_ms INTEGER NOT NULL DEFAULT 0, ttfb_ms INTEGER NOT NULL DEFAULT 0,
		msg_count INTEGER NOT NULL DEFAULT 0, prompt_tokens INTEGER NOT NULL DEFAULT 0, completion_tokens INTEGER NOT NULL DEFAULT 0,
		cached_tokens INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', req_body TEXT NOT NULL DEFAULT '', resp_body TEXT NOT NULL DEFAULT '')`)
	db.Exec(`INSERT INTO requests(ts,session_id,session_source,client_key_id,upstream_key_id,method,path) VALUES(1,'s','new',1,1,'POST','/x')`)
	db.Close()
	st, err := OpenStore(p)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var a int
	var l string
	if err := st.db.QueryRow(`SELECT attempts, attempt_log FROM requests`).Scan(&a, &l); err != nil || a != 1 {
		t.Fatal(a, l, err)
	}
}
