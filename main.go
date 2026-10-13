package main

import (
	"context"
	_ "embed"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

//go:embed web/index.html
var indexHTML []byte

//go:embed web/login.html
var loginHTML []byte

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	password := os.Getenv("ADMIN_PASSWORD")
	if password == "" {
		log.Fatal("ADMIN_PASSWORD is required")
	}
	dbPath := env("DB_PATH", "data/proxy.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		log.Fatal(err)
	}
	st, err := OpenStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()
	os.Chmod(dbPath, 0o600) // the DB holds upstream API keys

	rs := NewResolver(st)
	proxy := NewProxy(st, rs, env("UPSTREAM_URL", "https://opencode.ai/zen/go/v1"))
	admin := NewAdmin(st, password)

	mux := http.NewServeMux()
	mux.Handle("/v1/", proxy)
	admin.Routes(mux)
	page := func(w http.ResponseWriter, r *http.Request) {
		if !admin.authed(r) {
			http.Redirect(w, r, "/admin/login", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(indexHTML)
	}
	mux.HandleFunc("GET /admin", page)
	mux.HandleFunc("GET /admin/{$}", page)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/admin", http.StatusFound) })

	go func() {
		for range time.Tick(time.Minute) {
			st.ExpireSessions()
		}
	}()
	go func() {
		for {
			st.PruneRequests(st.SettingInt("request_retention_days", 30))
			time.Sleep(time.Hour)
		}
	}()

	srv := &http.Server{Addr: env("LISTEN", ":8080"), Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("listening on %s (dashboard: /admin, proxy: /v1)", srv.Addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
