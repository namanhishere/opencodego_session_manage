# OpenCode Go session proxy

OpenAI-compatible proxy in front of [OpenCode Go](https://opencode.ai/docs/go/) that makes sure every
request carries a correct, stable `x-opencode-session` header (used by OpenCode for routing and prompt
caching), pools several OpenCode Go API keys, and gives you an admin dashboard for usage and tracing.

## Run

    ADMIN_PASSWORD=change-me go run .          # dashboard: http://localhost:8080/admin
    # or: docker compose up -d --build   (needs ADMIN_PASSWORD in .env)
    # or: docker build -t ocproxy . && docker run -p 8080:8080 -v ocdata:/data -e ADMIN_PASSWORD=... ocproxy

1. Open `/admin`, add your OpenCode Go key(s) under **Upstream keys**.
2. Create a key for your agent under **Client keys** (shown once).
3. Point the agent at `http://<host>:8080/v1` with that key. Both `/v1/chat/completions` and `/v1/responses` work.

Env: `ADMIN_PASSWORD` (required), `LISTEN` (`:8080`), `DB_PATH` (`data/proxy.db`), `UPSTREAM_URL` (`https://opencode.ai/zen/go/v1`).
Settings (dashboard → Settings) also let you turn off request logging entirely (`log_requests`) or ignore the client's `x-opencode-session` header and always derive the session from the conversation (`ignore_client_session`).
Put it behind TLS (reverse proxy) if it is not on a private network.

## How a request is mapped to a session

1. `x-opencode-session` sent by the client, scoped to the client key.
2. **Message-chain match**: each request's `messages` (or responses `input`) is hashed as a prefix chain.
   Chat is append-only, so turn N+1 begins with turn N's full list; the longest stored prefix wins.
   No response parsing needed, so it works with streaming.
3. **System prompt + first user message** hash, as fallback when the client rewrites history
   (compaction, stripped reasoning/tool output).
4. Otherwise a new session is created.

Known limits: two conversations with identical starts share a session until they diverge (they share
a prompt prefix anyway); `previous_response_id` (responses API) is not tracked yet; after history
compaction the fallback may keep the session, but the provider cache is cold regardless.

A session is pinned to one upstream key for its lifetime; new sessions go to the enabled key with the
fewest active sessions. There is deliberately no failover on 429/quota errors — switching keys mid
conversation loses the cache and would amount to evading per-key limits. Session TTL (default 24 h,
sliding) is configurable in Settings; expired sessions stay visible for tracing until request retention
(default 30 days) removes them.

## Dashboard

Overview has charts (requests, tokens, latency, cache hit, stop reasons, status) and can be filtered by client key and/or upstream key; click a row in the key tables to filter. Click any request for its stop reason and, for streams, a waterfall timeline (upstream wait, first-token wait, reasoning / text / tool-call spans, chunk gaps). ↻ Reload (or `r`) refreshes the current view; the selector next to it sets auto-refresh.

Upstream keys are stored in plaintext in the SQLite file (mode 0600); protect the volume.
