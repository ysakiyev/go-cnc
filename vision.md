# Plan: go-cnc roadmap — scaling refactor + fleet + MCP federation

## Vision (one page)

**Today:** single-process gRPC SOCKS5 broker. Proof-of-concept.

**Target:** the reverse-tunnel platform that lets AI agents (and humans) reach private infrastructure — with identity, policy, audit, and **MCP federation** so Claude Code / Claude Desktop can invoke tools inside any private network without VPN or firewall changes.

**Core primitive never changes:** agents dial out, broker routes authenticated streams. Everything below is accretive — each phase ships something independently valuable; nothing thrown away.

### Five phases

| Phase | Theme | PRs | What becomes possible after |
|-------|-------|-----|-----------------------------|
| 1 | Scaling foundation | PR 1-4 | horizontally scalable control + relay planes with Redis registries; current SOCKS5 functionality intact but production-grade |
| 2 | Fleet management | PR 5-8 | zero-touch enroll 100s of agents; tags + RBAC; operator CLI/TUI; audit log |
| 3 | MCP protocol head | PR 9-12 | Claude Code/Desktop can list agents and invoke core tools (shell, file, port_forward, log, metric) with policy gates |
| 4 | MCP passthrough federation | PR 13-15 | agents carry **any existing MCP server** (Postgres, GitHub, custom company MCPs) from inside private networks — the unique differentiator |
| 5 | Polish / launch | PR 16-18 | mTLS everywhere, observability, docs, demo — ready for external users |

### End-state positioning

> "Self-host go-cnc, connect agents to your fleet, then give Claude Code / Claude Desktop scoped access to operate them — with mTLS, RBAC, audit, and the full MCP ecosystem running inside your private network."

### Operator UX: MCP-first

**Decision:** Claude Code / Claude Desktop is the primary interactive operator surface. No bubbletea TUI, no web dashboard.

Rationale:
- Scope discipline. A TUI is a parallel product — every MCP tool improvement would need a TUI equivalent or they drift.
- On-brand. The thesis is "MCP-first platform"; operator UX being MCP-first is the thesis, not a contradiction.
- Claude Code is genuinely good. A custom TUI would compete with it.
- Demo narrative is stronger: "everything is natural-language ops" > "look at this TUI."

What stays:
- **`cnc` CLI** — thin, not TUI. Handles bootstrapping (`enroll`, `login`, `policy apply`), fallback when Claude Code isn't available, and CI/automation (`audit export`, `agents list --json`). Reuses the gRPC client code; scope ~500 LOC.
- **Minimal approvals web page** — one htmx page for approvers who aren't at their dev machine. ~150 LOC.
- **Optional `cnc watch`** — a later add-on, tails the audit/event stream with colors. Added if and only if the "live fleet view" need is real after a few months of MCP-only use.

What's dropped:
- Bubbletea TUI (was PR 7).
- Web dashboard / audit browser UI (was PR 16).

Audit browsing happens via an MCP tool (`audit_search`) that Claude calls; results are rendered in the chat.

### Branching strategy

- Phase 1 lives on long-lived `scaling-refactor` branch. PRs 1-4 merge into `scaling-refactor`; once verified end-to-end, `scaling-refactor` merges to `main`.
- Phases 2-5 happen on `main` via short-lived feature branches per PR.

---

## Phase 1: Scaling foundation

### Context

The current go-cnc server is a single-process gRPC relay where control logic (agent registry, session dispatch) and data logic (per-tunnel byte pumping) live in the same `ConnManager` struct and share the same locks. See [pkg/conn/conn_manager.go](pkg/conn/conn_manager.go), where `Agents` (control) and `Conns` (data) sit next to each other, and [pkg/services/ClientService.go:39-74](pkg/services/ClientService.go#L39-L74), where `CreateConn` performs both registry lookup and agent notification synchronously.

This coupling caps the system at one server process. A crash drops every active tunnel, the in-memory agent map is a hard SPOF, and every byte of tunneled traffic must traverse the same binary that handles registration.

**Goal:** separate the two planes so each scales independently. Control plane becomes a stateless, horizontally-scalable service backed by Redis. Data plane becomes a stateless relay pool that pumps bytes between a client stream and an agent stream paired by a short-lived HMAC-signed session token. This is the go-cnc equivalent of the TURN pattern.

All Phase 1 work happens on a long-lived `scaling-refactor` branch cut from `main`. Each PR gets its own feature branch off `scaling-refactor` (e.g. `scaling-refactor/pr1-extract-relay`). `main` stays on the legacy single-node code as the rollback baseline, so within `scaling-refactor` the old `CreateTcpStream` RPCs and `ConnManager.Conns` can be deleted outright instead of living behind a flag.

### Target architecture

```
            ┌──────────────┐
  agents ──▶│   control    │◀── clients
            │  (stateless) │
            │   N replicas │
            └──────┬───────┘
                   │  Redis: agent:{id}, relay:{id}, control:{id}
                   │  (registries + TTL heartbeats)
                   ▼
            ┌──────────────┐
            │    relay     │◀── both client and agent dial here with
            │  (stateless) │    an HMAC-signed session token; relay
            │   N replicas │    pairs them by sessionId and io.Copy's
            └──────────────┘    bytes. No persistent state.
```

- **Control plane** never touches tunnel bytes after setup. It validates the client, picks a relay from `relay:*` by least session-count, mints an HMAC token for each of {client, agent}, dispatches `AgentConn{connId, relayAddr, sessionToken}` to the agent, and returns `{relayAddr, sessionToken}` to the client.
- **Agent's long-lived `CreateConnStream` is pinned to the control node it connected to** — server-streaming gRPC objects cannot be migrated. When control node A needs to dispatch to an agent whose stream lives on control node B, A calls `ControlInternal.ForwardAgentConn` on B over gRPC (not Redis pub/sub — pub/sub is lossy and has no acks).
- **Relay** has no knowledge of agents or clients; it only validates the token HMAC, matches by `sessionId`, and pumps bytes with `io.Copy` over stream-as-io-ReadWriter adapters.

### Proto changes (Phase 1)

Three files:

#### [proto/protocol.proto](proto/protocol.proto) (modified)

Add fields to `AgentConn` and `CreateConnResponse`. Remove `CreateTcpStream` from both services (on refactor branch only).

```proto
service ClientService {
  rpc GetAgents(Empty) returns (GetAgentsResponse);
  rpc CreateConn(ClientConnRequest) returns (CreateConnResponse);
  // CreateTcpStream removed — relay handles data plane now.
}

service AgentService {
  rpc CreateConnStream(Empty) returns (stream AgentConn);
  // CreateTcpStream removed.
}

message AgentConn {
  string connId        = 1;
  string remoteAddr    = 2;
  string relay_addr    = 3;  // new
  string session_token = 4;  // new
}

message CreateConnResponse {
  string connId        = 1;
  string relay_addr    = 2;  // new
  string session_token = 3;  // new
}
```

#### `proto/relay.proto` (new)

```proto
service RelayService {
  // Token goes in metadata key "session_token". Role is inside the token.
  rpc Tunnel(stream Chunk) returns (stream Chunk);
}
```

#### `proto/control_internal.proto` (new)

```proto
service ControlInternal {
  rpc ForwardAgentConn(ForwardAgentConnRequest) returns (ForwardAgentConnResponse);
}

message ForwardAgentConnRequest {
  string agent_id      = 1;
  string conn_id       = 2;
  string remote_addr   = 3;
  string relay_addr    = 4;
  string session_token = 5;
}

message ForwardAgentConnResponse {
  bool   delivered = 1;
  string error     = 2;
}
```

Regenerate with the existing `make gen` target after adding the new .proto files to the glob.

### HMAC session token

Format: base64url(body) + "." + base64url(hmac-sha256(body, key)).

Body packed struct:
```
sessionId   [16]byte  // same for both halves of a tunnel
role        byte      // 1=client, 2=agent
relayId     [16]byte  // binds token to one relay
expUnix     int64     // 30-60s after issue
keyId       byte      // for rotation
nonce       [8]byte
```

Shared secret `RELAY_SIGNING_KEY` loaded from env var on control and relay. Key rotation via `keyId` + `map[byte][]byte` of active keys.

Implementation in `pkg/authn/token.go`: `Mint(keys, claims) (string, error)`, `Verify(keys, token) (Claims, error)`. Pure Go, no external deps, <150 LOC.

### Redis schema (Phase 1)

- `agent:{id}` hash — `{control_node_id, desc, last_seen}` with 30s TTL; agent heartbeats refresh via a ticker in `pkg/agent/agent.go`.
- `relay:{id}` hash — `{addr, session_count, last_seen}` with 15s TTL; relay heartbeats from `pkg/relay/heartbeat.go`.
- `control:{id}` hash — `{internal_addr, last_seen}` with 30s TTL; control nodes publish their own internal-gRPC address so peer nodes can `ForwardAgentConn` to them.

All keys use TTL for liveness. No pub/sub. Redis is registry state only.

Client: `github.com/redis/go-redis/v9`.

### Relay pairing logic

`pkg/relay/registry.go`:

```go
type halfSession struct {
    role   Role
    stream pb.RelayService_TunnelServer
    ready  chan struct{}
}

type session struct {
    mu      sync.Mutex
    client  *halfSession
    agent   *halfSession
    paired  chan struct{}       // closed when both halves present
    done    chan struct{}       // closed when tunnel torn down
    ctx     context.Context
    cancel  context.CancelFunc
}

type Registry struct {
    mu       sync.Mutex
    sessions map[uuid.UUID]*session
}
```

Flow per `Tunnel` stream:

1. Verify token from metadata. Extract `{sessionId, role, relayId, exp}`. Reject if `relayId != self.id`, expired, or HMAC invalid.
2. `reg.Attach(sessionId, role, stream)` — create session if absent, attach to the right role slot, reject if slot already filled (replay protection). If both slots filled, close `paired`.
3. Wait on `paired` | `ctx.Done()` | 10s handshake timeout.
4. Once paired: spawn two `io.Copy`s using `streamReader`/`streamWriter` adapters in `pkg/relay/stream_io.go`. Cancel the shared context on any return.
5. On teardown: remove session from registry, decrement session_count in Redis, return from `Tunnel` (closes the stream).

Adapters:
- `streamReader`: `Read` calls `stream.Recv()`, buffers leftover bytes across calls.
- `streamWriter`: `Write` calls `stream.Send(&Chunk{Data: buf[:n]})`.

This replaces the 4-goroutine channel-pair pattern in [conn_manager.go:96-154](pkg/conn/conn_manager.go#L96-L154) with 2 goroutines per tunnel and no intermediate buffer.

### PR breakdown (Phase 1)

#### PR 1 — Extract relay in-process, behind new RPCs

- Add `proto/relay.proto`, regenerate.
- Add `pkg/authn/token.go` + `pkg/authn/interceptor.go`.
- Add `pkg/relay/` (registry, service, stream_io, server).
- Add `pkg/common/strings.go` entries for new log formats.
- Modify [cmd/server/main.go](cmd/server/main.go) to register `RelayService` alongside existing services.
- Modify [pkg/services/ClientService.go](pkg/services/ClientService.go) `CreateConn` to mint a token and return `{relayAddr, sessionToken}` (relayAddr = local address in this PR).
- Modify [pkg/agent/agent_handlers.go](pkg/agent/agent_handlers.go) `HandleConnStream` to dial the relay addr from `AgentConn` with the token, not use `CreateTcpStream`.
- Modify [pkg/client/client.go](pkg/client/client.go) `socks5Proxy` to dial the relay from `CreateConnResponse`, not use `CreateTcpStream`.
- Remove `CreateTcpStream` from `protocol.proto`, `ClientService`, `AgentService`, and delete the `Conns` half of `ConnManager`.
- Bump [go.mod](go.mod) to `go 1.22`; update direct deps for compatibility.
- Tests: `pkg/authn/token_test.go`, `pkg/relay/registry_test.go` (pair/replay/timeout), `pkg/relay/service_test.go` (in-memory e2e).

**Value:** Single binary still, but control and data are now separate code paths with a clean proto boundary. Verify with existing `run_*.sh` scripts.

#### PR 2 — Split into `control` and `relay` binaries

- Add `cmd/control/main.go` and `cmd/relay/main.go`.
- Keep [cmd/server/main.go](cmd/server/main.go) as an "all-in-one" dev entrypoint that starts both in one process.
- Move `RelayService` registration out of `pkg/server/server.go` into `pkg/relay/server.go`. Move `ClientService`/`AgentService` registration into `pkg/control/server.go`.
- Rename `pkg/conn/conn_manager.go` (now holding only `Agents` map) to `pkg/control/agents.go`.
- [config/config.go](config/config.go) + [config/server.go](config/server.go) grow `ControlConf`, `RelayConf` sections; keep `ServerConf` for backward compat in the dev binary.
- Update [Makefile](Makefile) with `control`, `relay` targets.
- Client learns relay addr from `CreateConn` response (already wired in PR 1), so no client change.

**Value:** Run `bin/control` + `bin/relay` on different ports locally; prove routing works end-to-end.

#### PR 3 — Redis-backed registries + HMAC token verification against Redis relay pool

- Add `pkg/registry/` with `AgentRegistry`, `RelayRegistry`, `ControlRegistry` interfaces and Redis implementations.
- Control on agent connect (`AgentService.CreateConnStream`): write `agent:{id}.control_node_id = self`; start heartbeat goroutine refreshing TTL every 10s.
- Control on startup: write `control:{self}.internal_addr`; heartbeat.
- Relay on startup: write `relay:{self}.addr`; heartbeat with current `session_count`.
- In `ClientService.CreateConn`: pick a relay via `RelayRegistry.Pick()` (random for v1; least-loaded by session_count is a one-line change later). Embed relayId in the token so the relay can check "this token is for me."
- Config gains `Redis: {addr, password, db}`.
- Tests: `pkg/registry/redis_*_test.go` against a local Redis (or miniredis).

**Value:** Single control still, but relay selection is dynamic. You can scale relays to N behind an L4 LB.

#### PR 4 — Multi-control-node dispatch via `ControlInternal`

- Add `proto/control_internal.proto`, regenerate.
- Implement `ControlInternal.ForwardAgentConn` in `pkg/control/internal_service.go` — verifies caller (see Auth below), looks up local agent stream, calls `Send()`.
- Add `pkg/control/dispatch.go`: `Dispatch(ctx, agentId, payload) error`. If `agent:{id}.control_node_id == self`, call local stream. Else look up `control:{peer}.internal_addr` and make a `ForwardAgentConn` gRPC call. If lookup fails or peer returns `Unavailable`, return error to the client — the client retries and the TTL will have swept the stale entry.
- Bound internal-gRPC connections with a small `map[peerId]*grpc.ClientConn` pool keyed by peer id, lazily dialed.

**Value:** True horizontal control-plane scale. Agents can connect to any control replica; clients can connect to any control replica; dispatch just works.

### Authentication (Phase 1 in-scope)

Three mechanisms added across PR 1 and PR 3:

1. **Session HMAC token** (PR 1) — relay-side, covered above.
2. **Agent pre-shared key** (PR 3) — fixes the current issue where an agent can self-declare any UUID. Config `Agent: { id, preshared_key }`. Agent sends `agent_id` + `agent_token = hmac(preshared_key, agent_id + timestamp + nonce)` in `CreateConnStream` metadata. Control has `AgentCredentials: map[string][]byte` (agent-id → key). gRPC stream interceptor rejects unknown or bad HMAC. Keys are loaded from config; rotation is restart-only for v1.
3. **Client bearer token** (PR 3) — simple shared-secret bearer token in `authorization` metadata. Control's unary+stream interceptor on `ClientService` rejects missing/wrong token. Suitable for v1; replace with per-client identities later if needed.

mTLS is *not* in Phase 1. Done properly in Phase 5.

### Config additions (Phase 1)

[config.yml](config.yml):

```yaml
Control:
  Address: "0.0.0.0:54321"
  InternalAddress: "0.0.0.0:54322"  # for ForwardAgentConn
  NodeID: "control-1"                # override via CONTROL_NODE_ID
Relay:
  Address: "0.0.0.0:54400"
  NodeID: "relay-1"
Redis:
  Addr: "localhost:6379"
  DB: 0
Authn:
  RelaySigningKey: "${RELAY_SIGNING_KEY}"
  ClientBearerToken: "${CLIENT_BEARER_TOKEN}"
  AgentCredentials:
    - id: "aaaa-...."
      key: "${AGENT_KEY_AAAA}"
```

### Verification (Phase 1)

Happy path:
1. Start Redis: `redis-server --daemonize yes`.
2. Set env: `export RELAY_SIGNING_KEY=$(openssl rand -hex 32); export CLIENT_BEARER_TOKEN=test; export AGENT_KEY_AAAA=$(openssl rand -hex 32)`.
3. Start two control replicas and two relays.
4. Start agent pointed at control1.
5. Start client pointed at control2 (cross-node dispatch case). Pick the agent; observe `ForwardAgentConn` gRPC call in control2's logs.
6. Run SOCKS5 traffic through client → relay pool → agent → target. Verify: `curl -x socks5://localhost:9999 https://example.com`.

Failure paths:
- Kill relay1 mid-tunnel → client reconnect should land on relay2.
- Kill control1 → agent reconnects to control2; next client `CreateConn` no longer needs forwarding.
- Wrong token → relay returns `Unauthenticated`.
- Replayed token (same role dialing twice) → relay returns `AlreadyExists`.

---

## Phase 2: Fleet management

**Why:** Phase 1 gives a scalable dumb relay. Phase 2 turns the server into something that can manage 10s→1000s of agents with identity, tags, RBAC, and operator UX. Prerequisite for MCP (Phase 3) because every MCP tool call must resolve `(operator, policy, agent, capability) → allow/deny` — policy engine needs to exist first.

### PR 5 — Zero-touch enrollment + agent identity (internal CA)

Replace pre-shared keys (Phase 1 stopgap) with mTLS + short-lived certs from a built-in CA.

- New package `pkg/ca/`: tiny internal CA (crypto/x509). One root cert + signing key stored on control plane. Config `Control.CA: { cert, key, ttl }`.
- New HTTP endpoint on control plane `POST /enroll` accepting `{bootstrap_token, hostname, public_key}`. Verifies token (single-use, 15-min TTL, stored in Redis `enroll:{token}`), signs pubkey, returns `{agent_cert, agent_id, ca_cert}`.
- New CLI command: `cnc enroll token new --tags env=prod,role=web --ttl 15m` → prints a short token.
- New installer script: `curl -sSL $URL/install.sh | sh -s -- --enroll TOKEN --control HOST`. Drops binary + systemd unit + runs `go-cnc enroll` on first boot.
- Agent swaps: reads local cert from disk, establishes mTLS gRPC to control, stream interceptor on control validates the client cert against the CA (replaces the pre-shared HMAC check from PR 3).

**Value:** From now on, a new machine becomes a fleet member with one command. No manual config editing, no per-agent keys.

### PR 6 — Tags, capability declaration, RBAC policy engine

- Extend `agent:{id}` Redis hash with `tags` (list of `key=value`) and `capabilities` (list of capability names).
- Proto: extend `CreateConnStream` first message to include `AgentHello{tags, capabilities, version}` (before the server starts sending `AgentConn` messages).
- Policy engine in `pkg/policy/`:
  - `Policy` struct: `{name, subjects, agent_selector, tools, approval_required, ttl}`.
  - `Evaluator.Allow(operator, agent_id, tool) → (allow bool, approval_required bool)`.
  - Loaded from YAML file on control plane startup; hot-reloadable via SIGHUP.
  - Subjects: group names resolved from the operator's bearer token / cert claim.
  - Agent selector: simple label matcher for v1 (`env=prod AND role=web`); CEL later.
- Extend `ClientService.CreateConn` to consult the policy engine: reject if operator can't reach the requested agent.
- New unary RPC `ClientService.ListAgents(filter) → AgentList` replacing the bare `GetAgents`, filters by operator's visible-agent set and applies selector from request.

**Value:** Multi-tenant usable. Different operators see different slices of the fleet. Different policies control what each can do.

### PR 7 — Operator CLI (`cnc`)

- New `cmd/cnc/main.go`. Subcommands:
  - `cnc login` — obtain/refresh operator cert from the CA.
  - `cnc enroll token new --tags ... --ttl ...` — mint bootstrap tokens for new agents.
  - `cnc agents list [--tag k=v] [--json]` — query the fleet (reads Redis via control plane).
  - `cnc agents get <id>` — detail for one agent.
  - `cnc shell <agent_id> -- <cmd>` — ad-hoc execution via the same dispatch path MCP uses.
  - `cnc socks --agent <id> --listen 127.0.0.1:9050 [--persistent]` — the local SOCKS5 daemon for long-running apps (Telegram, SSH, etc.). See Phase 3 socks section.
  - `cnc policy apply <file.yaml>` — push a policy.
  - `cnc audit search [--operator] [--agent] [--since] [--json]` — audit queries for CI/compliance export.
- All subcommands are thin gRPC clients; reuse the `pkg/client` connection machinery.
- No TUI. No interactive menus. Scripts-friendly output by default (`--json` for structured).
- Replace existing interactive agent-picker in `cmd/client/main.go` with a pointer to `cnc socks` for the SOCKS5 case; `cmd/client` stays as a legacy thin wrapper.

**Value:** Bootstrapping + CI + fallback path covered. Zero UI framework dependencies.

### PR 8 — Audit log + approval gates + minimal approvals web

- **Audit log.** Append-only. Backing store abstracted (`pkg/audit.Sink`); impls: `stdout` (default), `file` (rotate), `sqlite` (queryable), `clickhouse`/`s3` later. Every control-plane operation that touches an agent writes `{ts, operator_id, agent_id, op, args_hash, policy, result}`. Entries signed with the CA key so they're tamper-evident when exported.
- **Approval gates.** When a tool call has `approval_required`, control plane returns `Pending{approval_id}` instead of dispatching. Requester side blocks or polls. Approver receives notification via Redis pub/sub `approvals:{operator}` (pub/sub is fine here — best-effort notification, unlike dispatch). Approver calls `ClientService.Approve(approval_id, true|false)`. If approved, dispatch proceeds; otherwise rejected. Audit entry written either way.
- **Minimal approvals web page.** New `pkg/web/approvals.go`: single htmx page at `/approvals` served by the control plane, ~150 LOC. Lists pending approvals for the authenticated operator (auth via session cookie, same session tokens as MCP). Shows requester, agent, tool, arguments, requested-at. Approve/Reject buttons POST to an endpoint that calls the same `Approve` RPC. No SPA framework, no JS bundler — htmx + `templ` or plain `html/template`.

**Value:** Compliance-grade. Enterprise-sellable story: "who ran what, where, when, approved by whom." Approvers don't need Claude Code to click a button.

---

## Phase 3: MCP protocol head

**Why:** This is where the MCP pivot starts. After Phase 3, Claude Code/Desktop can connect to go-cnc and invoke core tools across the fleet with full RBAC and audit. But Claude only sees the *built-in* capabilities, not arbitrary MCPs yet (that's Phase 4).

### PR 9 — MCP server endpoint + session auth

- Add MCP library dep: `github.com/modelcontextprotocol/go-sdk` (or equivalent; verify current state of the ecosystem at implementation time).
- New `pkg/mcp/` package with a server that speaks MCP's streamable-HTTP transport on control plane's MCP port (default `:54500`).
- Auth: on `initialize`, client presents an MCP session token (separate from Phase 1's session HMAC — this is operator-scoped, long-lived). Tokens minted by `cnc mcp session new --ttl 8h --policy sre-oncall` → copy into Claude Code config.
- Session token maps to an operator identity + RBAC policy, same engine as Phase 2. Every subsequent `tools/list` / `tools/call` evaluates against this policy.
- `tools/list` returns a seed set (just `agent_list` for this PR); proof of plumbing. Later PRs add more tools.

**Value:** Claude Code can list your agents. Not useful yet, but the auth and dispatch plumbing is real.

### PR 10 — Core agent capabilities as MCP tools

Expose the always-available agent capabilities as MCP tools. Each tool is `agent_<capability>` taking at least `agent_id`.

Built-in capabilities (agent-side, new `pkg/agent/capabilities/`):
- `shell` — execute command, stream stdout/stderr/exit
- `file` — read/write/list/stat/delete (allow-list of paths per policy)
- `port_forward` — open a reverse tunnel to a local target, return `localhost:PORT` on the MCP client side (this reuses Phase 1's relay — control plane opens a session between the Claude-side bridge and the agent)
- `process` — list, kill, signal
- `log` — tail, grep, streaming
- `metric` — read Prometheus endpoint via localhost, return parsed

New gRPC RPC `AgentService.InvokeCapability(stream InvokeRequest) returns stream InvokeResponse` for tool dispatch (bidi to support streaming tools like `shell` and `log tail`).

Control plane MCP handler for `tools/call`:
1. Parse `agent_id`, capability, args.
2. Policy check.
3. If `approval_required`: suspend, notify operator, resume on approve.
4. Dispatch to agent via `InvokeCapability` through the existing control-plane dispatch path (local or `ForwardAgentConn` to peer).
5. Stream response back over MCP.
6. Audit entry.

**Value:** All the scenarios we sketched (shell to a box, tail logs, port-forward a DB). Claude Code is now a real fleet operator.

### PR 11 — Platform/data capability modules (opt-in per agent)

Agents can declare additional capabilities at enrollment. Each is a Go module under `pkg/agent/capabilities/` with a registered factory:

- `docker` — list/exec/logs/inspect (requires Docker socket access on the host)
- `systemd` — unit status/start/stop/restart/reload
- `kubectl` — wraps `kubectl` against a kubeconfig on the agent
- `git` — repo ops in a checkout
- `http` — HTTP client from agent's network vantage point
- `database` — `psql`/`mysql`/`redis-cli` wrappers with per-policy query allow-list

Operator enables via `cnc agents enable-capability <id> docker` which writes to `agent:{id}.capabilities` and the agent reloads on next heartbeat.

**Value:** Fleet becomes useful for concrete ops: "kubectl rollout restart deployment X on cluster-prod-eu," "docker logs of my edge devices," etc.

### PR 12 — Audit as MCP tools, richer annotations, rate limiting

- `audit_search(operator?, agent?, tool?, since?, until?, limit?)` MCP tool. Claude runs audit queries and renders results in chat. Replaces what a TUI audit browser would have done.
- `fleet_summary()` MCP tool — returns the top-level fleet overview (agent counts by tag, offline/stale counts, pending approvals) as a structured object. Claude uses this to answer "what's on my fleet?" without fanning out `agent_list` every time.
- Richer MCP tool annotations so Claude Code renders better: `title`, `destructive`, `idempotent`, `cost` hints (surfaced from policy).
- Rate limiting on per-operator tool calls to prevent runaway loops (policy-driven; default 60 calls/min).
- Optional `cnc watch` subcommand — tails the audit/event stream with colors in the terminal. Zero framework dependency. Ship only if, after a few months, you actually miss the "live view" feel.

**Value:** Phase 3 ships as a coherent MCP-native product. Claude is the UI; audit, search, and summary are themselves tool calls, not parallel UIs.

---

## Phase 4: MCP passthrough federation (the differentiator)

**Why:** Nobody else combines reverse-tunnel reachability with MCP federation. After this, anyone's existing MCP server (Postgres, GitHub, a company's custom one) works from inside a private network with zero firewall changes.

### PR 13 — `mcp-passthrough` capability (subprocess manager)

- New agent capability `mcp-passthrough` in `pkg/agent/capabilities/mcp/`.
- Agent config gains:
  ```yaml
  mcp_passthrough:
    - name: postgres
      command: ["npx", "-y", "@modelcontextprotocol/server-postgres", "postgres://..."]
      env: { ... }
    - name: github
      command: ["docker", "run", "--rm", "-i", "ghcr.io/github/github-mcp"]
      env: { GITHUB_TOKEN: "${GITHUB_TOKEN}" }
  ```
- Subprocess manager: spawns each configured MCP on agent startup, holds stdin/stdout, supervises restarts with backoff, propagates env.
- On startup, agent calls each local MCP's `initialize` + `tools/list`, caches schemas.
- Agent sends extended `AgentHello` including each passthrough's `{name, tools_schema_hash}`.
- Control plane stores `agent:{id}.passthroughs:{name}` with cached schema.
- New gRPC bidi stream `AgentService.ProxyMCP(stream MCPEnvelope) returns stream MCPEnvelope` — carries JSON-RPC envelopes through the tunnel, one pair per active MCP client-to-server session.

**Value:** Agents can now host MCP servers. Nothing consumes them yet.

### PR 14 — Tool namespacing + discovery + routing

- Control plane MCP server's `tools/list` merges:
  - built-in capabilities (Phase 3): `agent_shell`, `agent_file_read`, etc.
  - per-agent passthroughs: `{agent_id}.{passthrough_name}.{tool_name}` (e.g. `web-prod-1.postgres.query`, `bastion-eu.acme-custom.foo`).
- When Claude calls `web-prod-1.postgres.query`:
  1. Parse namespace, resolve to `(agent_id=web-prod-1, passthrough=postgres, tool=query)`.
  2. Policy check against tool name and agent tags.
  3. Open (or reuse) `ProxyMCP` stream to that agent.
  4. Send JSON-RPC `tools/call{name: "query", ...}` through the stream.
  5. Stream response back unchanged.
  6. Audit entry includes the passthrough name and tool name.
- `tools/list` is cached per agent with TTL; changes to an agent's passthrough config trigger a cache invalidation via `AgentHello` version bump.

**Value:** Claude can now invoke **any MCP server** running inside private networks, namespaced by agent. This is the differentiator — nobody else has it.

### PR 15 — Packaging: curated MCP recipes, one-command installer, Claude Code quickstart

- `recipes/` directory with tested config snippets for common MCPs: Postgres, GitHub, Sentry, Kubernetes, Linear, Slack, Filesystem, Puppeteer, etc. Each recipe = YAML fragment plus a README with caveats.
- `cnc recipe add postgres --url postgres://...` merges a recipe into agent config and restarts the passthrough.
- Refine the installer script (`curl | sh`) to prompt for MCP recipes to enable at enrollment.
- Quickstart doc: "from zero Claude Code to querying Postgres inside a private VPC in 3 commands." This is the demo.

**Value:** User onboarding compresses from "read 3 docs and yak-shave" to "paste one command + recipe." Required for anyone-but-you to actually use the thing.

---

## Phase 5: Polish / launch

### PR 16 — mTLS everywhere, cert rotation, audit export

- mTLS on all internal hops: control↔control (already in Phase 1), control↔relay (new), agent↔control (new, replacing the stopgap HMAC from PR 3 if not already done in PR 5).
- Automatic cert rotation (re-enrollment via the same CA) before expiry.
- Signed audit log export (JSONL with CA signatures) for compliance consumers (SIEM, S3, etc.).

### PR 17 — Observability

- Prometheus endpoint on every component (control, relay, agent) with metrics: `active_sessions`, `tunnel_bytes_total`, `mcp_tool_calls_total{tool,result}`, `dispatch_latency_seconds`, `policy_denies_total`.
- Structured logging (JSON) with trace IDs linking an MCP tool call through control → agent → passthrough.
- Optional OpenTelemetry for spans.
- Health endpoints `/healthz`, `/readyz`.

### PR 18 — Docs, quickstart, demo video, README makeover

- Rewrite README top-to-bottom: positioning, architecture diagram, quickstart, recipe catalog.
- Screencast/GIFs in README showing: enroll an agent, open Claude Code, query a private DB via passthrough.
- `docs/` with: architecture, operator guide, security model, contributing, FAQ.
- Sample compose/k8s manifests for self-hosters.
- Launch announcement (HN / r/selfhosted / X).

---

## Critical files (cumulative, Phase 1)

### Modified
- [proto/protocol.proto](proto/protocol.proto), [proto/pb/protocol.pb.go](proto/pb/protocol.pb.go)
- [pkg/services/ClientService.go](pkg/services/ClientService.go), [pkg/services/AgentService.go](pkg/services/AgentService.go)
- [pkg/conn/conn_manager.go](pkg/conn/conn_manager.go) (shrinks; renamed in PR 2)
- [pkg/agent/agent_handlers.go](pkg/agent/agent_handlers.go), [pkg/client/client.go](pkg/client/client.go)
- [cmd/server/main.go](cmd/server/main.go), [config/config.go](config/config.go), [config/server.go](config/server.go), [config.yml](config.yml)
- [go.mod](go.mod), [Makefile](Makefile)

### New (Phase 1)
- `proto/relay.proto`, `proto/control_internal.proto`
- `pkg/authn/token.go`, `pkg/authn/interceptor.go`
- `pkg/relay/{server,service,registry,stream_io,heartbeat}.go`
- `pkg/control/{server,agents,dispatch,internal_service}.go`
- `pkg/registry/{registry,redis_agent,redis_relay,redis_control,selector}.go`
- `cmd/control/main.go`, `cmd/relay/main.go`

### New packages introduced in later phases (sketch)
- Phase 2: `pkg/ca/`, `pkg/policy/`, `pkg/audit/`, `cmd/cnc/` (CLI only), `pkg/web/approvals.go` (htmx approvals page)
- Phase 3: `pkg/mcp/`, `pkg/agent/capabilities/*`
- Phase 4: `pkg/agent/capabilities/mcp/` (subprocess manager), `recipes/`
- Phase 5: `pkg/metrics/`, `docs/`

---

## Unit test strategy

Phase 1 ships the first tests (codebase has none today). Going forward:
- Every new package gets tests in the same PR.
- `pkg/authn`, `pkg/policy`, `pkg/audit` are pure logic → high coverage.
- `pkg/relay`, `pkg/control` dispatch → table tests + bufconn in-process.
- `pkg/registry` → miniredis.
- `pkg/mcp` → spec-conformance tests using a known-good reference MCP client.
- `pkg/agent/capabilities/mcp` → spawn a small mock MCP subprocess, assert JSON-RPC round-trip.
- E2E: `scripts/e2e.sh` spins Redis + control + relay + agent + mock target, runs through SOCKS5 and (from Phase 3) MCP tool calls.

---

## Risks / weaknesses called out

- **Agent stream pinning is a design constraint, not a bug.** If a control node crashes, its N agents must reconnect. Clients see `Unavailable` on in-flight `CreateConn` until the TTL expires and the agents land on a new node. Expected drop window: ~30s (TTL). Document this.
- **No session persistence across relay crashes** — a relay crash drops every tunnel on it. Acceptable for v1; matches existing behavior.
- **Bearer token for clients is coarse (Phase 1)** — anyone with the token can list agents and open any tunnel. Multi-tenant RBAC lands in Phase 2; don't ship Phase 1 to multi-operator production.
- **Shared HMAC key rotation is restart-gated (Phase 1).** Fine for infrequent rotation; zero-downtime rotation needs live-reloadable key store. Revisit in Phase 5.
- **Phase 3+ authentication footprint grows.** Operator tokens, agent certs, relay HMAC, inter-control mTLS — four different credential types. Document the full chain in `docs/security.md` before external users arrive.
- **Passthrough MCPs are trusted code.** An agent running a buggy or malicious MCP subprocess can do whatever that MCP can do — sandboxing is the user's responsibility (OS-level: user namespaces, cgroups, seccomp). Document this sharply.
- **Tool namespace collisions.** Two agents both exposing a `postgres` passthrough will produce `web-prod-1.postgres.query` and `db-staging.postgres.query`, which is fine, but naming consistency matters for operator UX. Recipe conventions help.
- **MCP ecosystem instability.** MCP spec + SDKs are evolving. Pin the go-sdk version, test against a matrix, be prepared to track breaking changes in Phase 3-4.
- **Scope discipline.** The gap between Phase 1 (scaling) and Phase 4 (MCP differentiator) is many months of focused work. Resist adding scope (web UI, second language SDKs, federation across orgs) until Phase 4 ships and external users ask for them.
