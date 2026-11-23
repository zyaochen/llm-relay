# llm-relay

One local base URL for every LLM gateway. Claude Code, Codex and anything else
speaking the Anthropic Messages or OpenAI Chat/Responses wire formats point at
`http://127.0.0.1:18765`; the relay reads `model` from each request, picks the
gateway that serves it, swaps in that gateway's credential and streams the
response back untouched.

There is **no protocol translation**: it only works with gateways that already
speak the protocol the client uses (the gateways it was built for serve
`/v1/messages`, `/v1/chat/completions` and `/v1/responses`). In exchange,
thinking signatures, SSE events and tool calls pass through byte-for-byte.

## Install

```sh
go build -trimpath -o ~/.local/bin/llm-relay .
llm-relay init            # ~/.config/llm-relay/{config.yaml,client.key}, never overwrites
$EDITOR ~/.config/llm-relay/config.yaml
llm-relay install-agent   # launchd: starts at login, restarts on crash
llm-relay routes          # which provider serves which model
```

Logs: `~/Library/Logs/llm-relay.log` (one line per request; never tokens).

## Add a gateway

Append a provider. Its models are discovered from `GET /v1/models` within
seconds — the config is hot-reloaded, no restart, and an invalid file is
rejected while the running config keeps serving.

```yaml
providers:
  - name: newgw
    base_url: https://newgw.example.com
    auth:
      cmd: [security, find-generic-password, -s, llm-relay/newgw, -w]
      # or: env: NEWGW_TOKEN      header: x-api-key   ttl: 5m
    # discover: false            # gateway has no /v1/models
    # models: [some-model]       # served in addition to discovered ones
```

Store the secret once: `security add-generic-password -U -s llm-relay/newgw -a "$USER" -w`.

Each request's model (a `[1m]`-style suffix is ignored) gets an ordered
candidate list, shown by `llm-relay routes`:
1. providers of the first matching `routes` glob, in listed order
2. every other provider serving the model (discovered or `models:`), in provider order
3. only if nobody serves it: `default_provider`, else `400` listing known models

## Failover (same model, other gateway)

When the first candidate fails, the same request goes to the next one. The
client resends the full conversation every turn, so nothing is lost; the
provider is picked before any byte reaches the client, so it never sees half
an answer. Only the prompt cache is cold after a switch.

| Upstream answer | Effect |
|---|---|
| 402, or 429/403/400 mentioning quota/balance/credit | next provider; skip this one for `quota_cooldown` (30m) |
| 429, 401 after token refresh, 403, 408, 5xx, network/TLS error | next provider; skip for `cooldown` (1m) |
| 404 | next provider, no cooldown (stale model list) |
| other 4xx (bad request, too large) | returned to the client unchanged |
| `event: error` overloaded/rate-limit mid-stream | passed through; provider cools so the client's retry lands elsewhere |

`Retry-After` wins when longer. When every candidate is cooling, the one that
recovers soonest is still tried. Cooldowns are per provider+model and survive
config reloads; after one expires the next request probes the original
provider again. `llm-relay reset [provider]` ends them now. The serving
provider is in the `X-Relay-Provider` response header, the path taken in
`X-Relay-Failover`, and every switch is logged at WARN.

To fail over a model, it only has to be served by two providers:

```yaml
routes:
  - {match: "claude-*", providers: [gw-a, gw-b]}
```

## Lossy networks

Some gateways sit behind slow or lossy paths (measured to one cross-border,
Cloudflare-fronted gateway: TLS handshakes 0.6–14 s, up to a third never finishing). The relay absorbs this so
CLIs don't fail:

| Layer | What it does | Setting (`network:`) |
|---|---|---|
| Hedged TLS dial | a handshake still running after 2 s is raced by another on the next IP, up to 3; first wins | `hedge_after: 2s`, `max_parallel: 3`, `connect_timeout: 15s` |
| Keep-warm | providers idle for 30 s get a cheap `GET /v1/models`, so requests reuse a live HTTP/2 connection and never wait for a handshake | `keep_warm: 30s` (0 disables) |
| Health pings | HTTP/2 connections quiet for 45 s are pinged; no pong in 20 s → dropped before a request lands on them | fixed |
| Lanes | each provider gets independent connections; a new request avoids a lane that has requests in flight but no byte for 5 s, then picks the least busy; quiet traffic stays on lane 0 with the others warm as standbys | `connections: 2` (1–4) |
| Same-provider retry | no response at all (reset, EOF, dial failure) → resend to the same provider, on another lane, before failing over | `retries: 1` |
| Concurrency limit | at most `max_concurrent` requests in flight per provider (a whole stream holds a slot); the rest queue FIFO in the relay instead of drawing the gateway's 429. A concurrency 429 that still arrives (another client on the same key) is resent to the same provider for up to 60 s, no cooldown, no failover | `providers[].max_concurrent` (e.g. 4) |
| Outage hold | no provider gives any HTTP answer (network down: DNS/TLS failing everywhere) → the request is held and retried (1 s doubling to 10 s) until the network is back; the client just sees a slow reply | `outage_wait: 3m` (0 = fail fast) |
| Stale-if-error DNS | resolver fails → the last good answer (≤ 24 h) is used; counted as `stale_dns` in status | — |
| Retryable failure | still unreachable after the hold → `503` + `Retry-After: 3` + `x-should-retry: true`, which Claude Code and Codex retry (10 tries for Claude Code, so a held relay stretches its tolerance from a few minutes to ~35) | — |

`llm-relay status` shows `outage_since` (no provider reachable since then) and
`last_upstream_ok`, which tells a network outage from a broken gateway: if both
gateways stopped answering at once and `stale_dns` climbs, it is the local
network (VPN / Wi-Fi), not the gateways.

Measure a path with `llm-relay probe [-n 20] <provider>` (fresh single vs
hedged handshakes); `llm-relay status` shows per-provider dial counters — few
dials relative to requests means connections are being reused.
`LLM_RELAY_DEBUG=1` logs keep-warm results and connection reuse.

`llm-relay status` also lists each lane's in-flight requests and whether it
is stalled; the `lane` field in each `proxied` log line says where a request
went.

When the upstream dies **before** the first response byte, the request is
retried on another lane / provider and the client never notices.

When it dies **mid-stream**, what helps depends on the client. Claude Code
recovers only when a stream ends with no content block completed — it then
re-asks without streaming; any other ending is final (measured: a clean end
after a completed block is accepted as the whole answer, an in-stream
`overloaded_error` and a dropped connection both become a final "API Error").
So the relay forwards Anthropic Messages streams whole-event and holds every
`content_block_stop` until just before `message_delta`; text and tool input
still stream live. If the upstream is lost, truncates the stream, or sends an
`event: error` before that point, the relay ends the response cleanly and
Claude Code fetches the answer again on its own (the lost partial is paid
twice). Other streams (Codex Responses) get an aborted connection, which Codex
retries. A restart drains in-flight requests (up to 90 s) instead of cutting
them.

What it cannot fix: packet loss still stalls the requests already on that
connection until TCP retransmits (lanes only keep it from spreading to new
ones), so those requests get slower, not failed. Curing that needs a better
route; HTTP/3 would help but UDP 443 was blocked on the network
it was built on.

**Thinking signatures.** Claude rejects a `thinking` block another vendor
signed (`400 Invalid signature`), e.g. after the history passed through kimi.
On exactly that error the relay retries the same provider without the foreign
thinking blocks, then without any; text, tool calls and tool results are kept.
Requests that are not rejected are never rewritten.

## Launch presets

The same config defines how clients are started, so gateways *and* launchers
live in one file. Each preset name becomes a shell command:

```yaml
default_preset: cta
presets:
  cta:                       # Claude Code
    client: claude
    description: Claude models
    model: "opus[1m]"        # ANTHROPIC_MODEL
    opus: claude-opus-5-5    # what /model opus|sonnet|haiku resolve to
    sonnet: claude-sonnet-5-5
    haiku: claude-haiku-4-5
    subagent: claude-haiku-4-5
    env: {CLAUDE_STREAM_IDLE_TIMEOUT_MS: "300000"}
  cxr:                       # Codex
    client: codex
    model: gpt-5.6-terra
    reasoning_effort: high
```

```sh
eval "$(llm-relay shell-init)"   # in ~/.zshrc: one alias per preset, plus ccr / ccp / ccnext
cta -p "hi"                      # = llm-relay run cta -p "hi"
cxr exec "hi"                    # Codex through the relay
ccnext cta                       # continue this directory's latest Claude session on cta
llm-relay presets                # list (ccp)
```

`run` execs the client directly (no wrapper process). It first clears every
gateway-related variable inherited from the shell, so a stale export cannot
redirect a session; then sets the relay URL, the local client key and the
preset's models. Codex gets the relay provider through `-c` overrides, so
`~/.codex/config.toml` needs no relay entry and the key never sits in the
shell. If the relay is down, `run` starts the launchd agent; it warns about
preset models no provider advertises. Bare `claude` / `codex` are untouched.

The client key only authenticates local tools to the relay; upstream tokens
never leave it.

## Troubleshooting

| Symptom | Check |
|---|---|
| `relay not reachable` | `launchctl kickstart -k gui/$(id -u)/io.github.chenziyaos.llm-relay`; if unloaded, `llm-relay install-agent` |
| `no provider serves model` | `llm-relay routes`; add a `routes` entry or `default_provider` |
| stuck on the backup gateway | `llm-relay routes` shows `[cooling …]`; `llm-relay reset` |
| 401 from a gateway | credential command output: run the `auth.cmd` by hand |
| config edit ignored | `grep "reload rejected" ~/Library/Logs/llm-relay.log` |
| upgrading the binary | `go build -trimpath -o ~/.local/bin/llm-relay . && llm-relay restart` — waits up to 2 min for a moment with no request in flight, else gives up; `-force` restarts anyway, draining in-flight streams up to 90 s while new connections wait (clients retry) |

## Develop

```sh
gofmt -l . && go vet ./... && go test -race ./...
```
