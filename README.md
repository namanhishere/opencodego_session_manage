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
fewest active sessions. There is deliberately no failover to another key on 429/quota errors — switching keys mid
conversation loses the cache and would amount to evading per-key limits. Session TTL (default 24 h,
sliding) is configurable in Settings; expired sessions stay visible for tracing until request retention
(default 30 days) removes them.

## Retries

Network errors and upstream `408/425/429/500/502/503/504/529` are retried on the **same** upstream key, up to
`retry_max` times (default 2, `0` = off) with exponential backoff (`retry_backoff_ms`, default 500, capped at 30 s).
A `Retry-After` header is honoured, but over 10 s the error is returned right away (quota exhausted, not a blip).
Retrying only happens before the response starts, so a stream that breaks midway is not replayed. Other 4xx are
returned unchanged. The `x-proxy-attempts` response header tells how many upstream calls were made; the Requests table shows it in a **Tries** column (e.g. `×3  429 → 429 → 200`).

Dashboard → **Active** lists requests currently being processed (waiting on the upstream, backing off before a retry with a countdown, or streaming) with their attempts so far.

## Models and model mapping

- `GET /v1/models` returns the upstream model list (fetched with an enabled upstream key, cached 5 min, served stale
  if the upstream is down) plus every alias from the mapping. Dashboard → Settings → *Fetch upstream models* refreshes it.
- **Model mapping** (Settings, one rule per line): `requested = target, fallback1, fallback2`. The request's `model` is
  rewritten to the first target; the next one is tried if the model is rejected (404, or 400/422 saying the model is
  unknown/unsupported) or still fails with a retryable error after the retries above.
- **Global fallback model** is appended to every chain, so it is also used for models the upstream does not know. Targets
  that are absent from a known upstream list are skipped while at least one other candidate remains.
- The model actually used is returned in `x-proxy-model` (only when it differs) and stored in the request log.

## Dashboard

Overview has charts (requests, tokens, latency, cache hit, stop reasons, status) and can be filtered by client key and/or upstream key; click a row in the key tables to filter. Click any request for its stop reason and, for streams, a waterfall timeline (upstream wait, first-token wait, reasoning / text / tool-call spans, chunk gaps). ↻ Reload (or `r`) refreshes the current view; the selector next to it sets auto-refresh.

Upstream keys are stored in plaintext in the SQLite file (mode 0600); protect the volume.
