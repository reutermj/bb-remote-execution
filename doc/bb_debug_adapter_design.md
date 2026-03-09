# bb_debug_adapter Design

## Overview

bb_debug_adapter is a gRPC service that relays Debug Adapter Protocol (DAP)
messages between a Bazel client and a remote worker running a process under a
DAP server (e.g., lldb-dap, debugpy). It is fronted by bb_storage like other
BuildBarn services.

DAP is transport-agnostic — it only requires that JSON messages are passed
bidirectionally between client and server. bb_debug_adapter acts as the
transport layer, buffering and relaying these messages through gRPC.

## Core Concepts

- **Session key**: Invocation ID (from Bazel's `tool_invocation_id`). One
  session = one client, one worker, one debug process.
- **Message format**: Raw DAP messages as opaque `bytes`. bb_debug_adapter does
  not parse or interpret DAP content.
- **Message buffer**: Per-session bidirectional buffer (client-to-worker,
  worker-to-client). Messages are buffered until consumed, enabling
  reconnection after transient disconnects.
- **Reconnection**: Uses DAP's native `seq` field. On reconnect, the
  client/worker reports the last `seq` it received and resumes from there.

## gRPC API

Two symmetric interfaces — one for clients, one for workers.

### Client-facing (fronted via bb_storage)

- `SendMessages(invocation_id, messages[])` — client pushes DAP messages
  (requests) to the session.
- `ReceiveMessages(invocation_id, last_seq)` — server-streams DAP messages
  (responses/events) from the worker. Supports resume via DAP `seq` after
  reconnect.
- `EndSession(invocation_id)` — client explicitly disconnects; triggers
  worker-side teardown.

### Worker-facing

- `RegisterSession(invocation_id)` — worker registers that a debug session is
  active for this invocation.
- `SendMessages(invocation_id, messages[])` — worker pushes DAP messages
  (responses/events) to the session.
- `ReceiveMessages(invocation_id, last_seq)` — server-streams DAP messages
  (requests) from the client. Same reconnect semantics.

## Bazel Configuration

The Bazel client is responsible for launching the process under a DAP server
using `--run_under`. A `.bazelrc` config bundles this with the platform
property that signals the runner:

```
build:debug --run_under=lldb-dap
build:debug --remote_default_exec_properties=debug_session=true
```

Usage: `bazel test --config=debug //my:test`

The DAP server binary (e.g., `lldb-dap`, `debugpy`) must be available on the
worker — either pre-installed in the worker image or provided as a Bazel-built
tool via a label in `--run_under`.

## Worker/Runner Modifications

- New platform property: `debug_session=true`. This is a boolean flag — the
  runner does not need to know which DAP server is being used.
- Runner detects the `debug_session` platform property and treats the
  launched process's stdin/stdout as DAP messages.
- Runner relays messages bidirectionally through bb_debug_adapter:
  - Process stdout -> `SendMessages` to bb_debug_adapter
  - `ReceiveMessages` from bb_debug_adapter -> process stdin
- `Run()` blocks until the debug session ends (debugger disconnect or process
  exit).

## Session Lifecycle

There are two client-side processes involved in a debug session:

- **Bazel**: Holds the remote execution action open. Does not speak DAP.
  If Bazel exits, the scheduler cancels the action and the worker dies.
- **VS Code (DAP client)**: Connects to bb_debug_adapter to exchange DAP
  messages with the worker. This is the "client" in the gRPC API above.

### Session ID coordination

The VS Code extension generates the invocation ID, passes it to the Bazel
CLI (via `--invocation_id`), and uses the same ID to connect to
bb_debug_adapter. This avoids any out-of-band discovery — the extension
controls both sides.

### Lifecycle steps

1. User initiates a debug session in VS Code. The extension generates an
   invocation ID.
2. The extension launches Bazel with `--config=debug` and
   `--invocation_id=<id>`. Bazel submits the action to remote execution with
   `--run_under` wrapping and `debug_session=true` platform property.
3. Scheduler assigns to worker normally. The action timeout should be set
   long enough to accommodate interactive debugging.
4. Runner detects `debug_session=true`, launches the action command (already
   wrapped with the DAP server), and calls `RegisterSession`.
5. Runner streams DAP messages bidirectionally through bb_debug_adapter.
6. The VS Code extension connects to bb_debug_adapter (via bb_storage
   frontend) using the same invocation ID.
7. VS Code and the worker exchange DAP messages through bb_debug_adapter.
8. Debug session ends (see Failure Modes below).

## Failure Modes and Cleanup

### Detecting dead connections

1. **Stream health**: Both `ReceiveMessages` streams carry periodic heartbeat
   keepalives. When a stream breaks, bb_debug_adapter detects the gRPC stream
   error immediately.
2. **Inactivity timeout**: If no messages flow in either direction for a
   configurable duration, bb_debug_adapter considers the session dead. This
   catches silent disconnects where neither side reconnects.

### Failure scenarios

- **VS Code disconnects**: bb_debug_adapter detects broken client stream or
  timeout. Sends a teardown signal on the worker's `ReceiveMessages` stream.
  Worker kills the DAP server process, `Run()` returns, action completes.
  VS Code can reconnect using DAP `seq`-based resume if the session is still
  alive.
- **Bazel dies**: Scheduler cancels the action, worker is killed. bb_debug_adapter
  detects the broken worker stream and signals VS Code with a termination
  error.
- **Worker dies**: bb_debug_adapter detects broken worker stream or timeout.
  Marks session as dead. VS Code receives a termination error.
- **bb_debug_adapter restarts**: All in-memory session state is lost. Both
  sides detect broken streams. Worker kills the debug process (action fails).
  This is acceptable since debug sessions are inherently ephemeral.
- **Normal exit**: User ends the debug session in VS Code (sends DAP
  `disconnect`). bb_debug_adapter relays to worker, DAP server exits,
  `Run()` returns, Bazel reports action completion.

## Constraints

- 1:1 mapping: one client, one worker, one debug session per invocation ID.
  No multi-client or multi-worker sessions.
- bb_debug_adapter does not interpret DAP messages — it is a pure relay.
- The DAP server must support stdin/stdout mode (most do).

## Runner-Side Implementation

### Overview

The runner codebase uses a decorator pattern — all runners implement the
`RunnerServer` interface (`Run()` + `CheckReadiness()`). The DAP relay can
be implemented as a new decorator that wraps an inner runner without
modifying `local_runner.go` or any existing runner code.

### New components

**1. DAP relay runner decorator** (`dap_relaying_runner.go`, ~100-150 lines)

A new `RunnerServer` decorator that intercepts `Run()`:

- Before calling the inner runner's `Run()`:
  - Calls `RegisterSession(invocation_id)` on bb_debug_adapter.
  - Sets up `cmd.Stdin` as a pipe (currently unused/nil in the base runner).
  - Captures `cmd.Stdout` via a pipe (instead of writing to a file).
- Spawns two goroutines for bidirectional relay:
  - **stdout → bb_debug_adapter**: reads DAP messages from the process's
    stdout pipe and calls `SendMessages`.
  - **bb_debug_adapter → stdin**: calls `ReceiveMessages` (server-stream)
    and writes incoming DAP messages to the process's stdin pipe.
- `Run()` blocks until the process exits or the debug session ends.

**2. Platform property detection**

The worker/build executor layer (upstream of the runner) checks for the
`debug_session=true` platform property. When present, it wraps the runner
with the DAP relay decorator. This is similar to how other decorators like
`temporary_directory_installing_runner` are conditionally applied.

**3. gRPC client for bb_debug_adapter**

The decorator holds a gRPC client for bb_debug_adapter's worker-facing API
(`RegisterSession`, `SendMessages`, `ReceiveMessages`). The connection
endpoint is provided via worker configuration.

### What stays unchanged

- `local_runner.go` — untouched. The decorator wraps it.
- All existing runner decorators — unaffected, they compose as before.
- Stdout/stderr file logging — when `debug_session` is not set, behavior is
  identical to today. The decorator is only applied conditionally.

## bb_debug_adapter Service Implementation

### Proto definitions

Two gRPC service definitions matching the gRPC API section above:

- `DebugAdapterClientService` — client-facing, fronted via bb_storage.
- `DebugAdapterWorkerService` — worker-facing, direct connection.

Message types:

- `DapMessage` — envelope containing opaque DAP `bytes` payload. The service
  never parses the content.
- `SendMessagesRequest` — `invocation_id` + repeated `DapMessage`.
- `ReceiveMessagesRequest` — `invocation_id` + `last_seq` (for reconnection
  resume).
- `ReceiveMessagesResponse` — a `DapMessage` or a heartbeat keepalive.
- `RegisterSessionRequest` / `EndSessionRequest` — `invocation_id`.

### Session store

In-memory map of invocation ID → session state. Each session contains:

- **Two message buffers**: client→worker and worker→client. Each buffer is
  an ordered list of `DapMessage` entries indexed by DAP `seq`. Messages are
  retained until the session ends to support reconnection replay.
- **Stream references**: the currently connected `ReceiveMessages` server
  streams for each side. Used to push new messages immediately when a
  consumer is connected.
- **Liveness state**: last activity timestamp for each side, whether each
  side is currently connected.
- **Mutex**: per-session lock for concurrent access (both sides read/write
  concurrently).

Sessions are created by `RegisterSession` and removed on teardown or
inactivity timeout.

### Message buffering and delivery

`SendMessages`:
- Appends messages to the appropriate directional buffer.
- If the receiving side has an active `ReceiveMessages` stream, signals it
  to deliver the new messages immediately.
- Updates the session's last activity timestamp.

`ReceiveMessages` (server-stream):
- On connect, replays any buffered messages with `seq` greater than the
  client's reported `last_seq`. This handles reconnection — the client
  picks up where it left off.
- After replay, blocks waiting for new messages. When `SendMessages` is
  called on the other side, the blocked stream is signaled and delivers
  the new messages.
- Sends periodic heartbeat keepalives on the stream so both sides can
  detect silent disconnects via stream health.
- On stream error or cancellation, updates the session's liveness state
  and triggers cleanup if appropriate.

This is the trickiest component — it needs to block efficiently (likely
via a condition variable or channel) while handling heartbeats,
reconnection replay, and clean teardown concurrently.

### Lifecycle management

- **`RegisterSession`**: Called by the worker. Creates the session entry in
  the store. Returns an error if the session already exists.
- **`EndSession`**: Called by the client. Marks the session for teardown.
  If the worker has an active `ReceiveMessages` stream, sends a termination
  signal on it so the worker knows to kill the DAP server process.
- **Inactivity reaper**: A background goroutine periodically scans sessions
  and removes any where no messages have flowed in either direction for
  longer than the configured timeout. This catches silent disconnects
  where neither side explicitly ends the session.
- **Stream error handling**: When a `ReceiveMessages` stream breaks (gRPC
  error), the session's liveness state is updated. If the broken side does
  not reconnect within a grace period, cleanup proceeds as described in
  Failure Modes.

### bb_storage fronting

The client-facing service (`DebugAdapterClientService`) is registered
behind bb_storage, consistent with other BuildBarn services. Clients
connect to the bb_storage frontend and are routed to bb_debug_adapter.

The worker-facing service (`DebugAdapterWorkerService`) is exposed
directly — workers connect to bb_debug_adapter without going through
bb_storage.

### Main binary

A new `cmd/bb_debug_adapter` binary with its own configuration proto:

- gRPC server endpoints (client-facing port, worker-facing port).
- Inactivity timeout duration.
- Heartbeat interval.
- bb_storage integration configuration.

The binary is stateless beyond the in-memory session store. No persistent
storage is required — debug sessions are ephemeral and do not survive
restarts.

## Service Configuration

BuildBarn services discover each other via `ClientConfiguration` fields in
their configuration protos. bb_debug_adapter follows this existing pattern.

### bb_debug_adapter configuration

New proto: `pkg/proto/configuration/bb_debug_adapter/bb_debug_adapter.proto`

```protobuf
message ApplicationConfiguration {
  // Servers for the client-facing DAP relay API (fronted via bb_storage).
  repeated buildbarn.configuration.grpc.ServerConfiguration client_grpc_servers = 1;

  // Servers for the worker-facing DAP relay API (direct connection).
  repeated buildbarn.configuration.grpc.ServerConfiguration worker_grpc_servers = 2;

  // How long a session can be idle before it is reaped.
  google.protobuf.Duration inactivity_timeout = 3;

  // Interval between heartbeat keepalives on ReceiveMessages streams.
  google.protobuf.Duration heartbeat_interval = 4;

  buildbarn.configuration.global.Configuration global = 5;
}
```

This mirrors the pattern in `bb_scheduler.proto` which has separate
`client_grpc_servers` and `worker_grpc_servers`.

### Worker configuration

Add a `ClientConfiguration` field to `bb_worker.proto` so the worker can
connect to bb_debug_adapter's worker-facing endpoint:

```protobuf
// In BuildDirectoryConfiguration or RunnerConfiguration:
buildbarn.configuration.grpc.ClientConfiguration debug_adapter = <N>;
```

This follows the same pattern as the existing `scheduler` field. The
worker passes this client connection to the DAP relay runner decorator.

### Example deployment configuration

```jsonnet
// bb_debug_adapter config
{
  clientGrpcServers: [{ listenAddresses: [':8985'] }],
  workerGrpcServers: [{ listenAddresses: [':8986'] }],
  inactivityTimeout: '600s',
  heartbeatInterval: '10s',
  global: { /* ... */ },
}

// bb_worker config (relevant additions)
{
  scheduler: { address: 'bb-scheduler:8983' },
  debugAdapter: { address: 'bb-debug-adapter:8986' },
  // ... existing fields ...
}
```

The client-facing port (8985) is registered behind bb_storage. The VS Code
extension connects through bb_storage using the same endpoint it uses for
other BuildBarn APIs. The worker-facing port (8986) is connected to
directly by workers.

## VS Code Extension

The extension acts as a DAP-to-gRPC bridge. From VS Code's perspective it
*is* the debug adapter — VS Code sends DAP messages to it, and instead of
forwarding them to a local process over stdio, it relays them over gRPC to
bb_debug_adapter.

### Extension configuration

The extension registers a debug type (e.g., `"bazel-remote-debug"`).
Configuration is provided via `launch.json`:

```json
{
  "type": "bazel-remote-debug",
  "request": "launch",
  "target": "//my:test",
  "bbStorageEndpoint": "bb-storage:8985",
  "bazelBinary": "${workspaceFolder}/bazelisk",
  "bazelConfig": "debug"
}
```

- `target` — the Bazel target to debug.
- `bbStorageEndpoint` — bb_storage frontend that routes to bb_debug_adapter.
- `bazelBinary` — path to the Bazel or Bazelisk binary. Defaults to `bazel`
  on `$PATH`. Supports VS Code variables like `${workspaceFolder}`.
- `bazelConfig` — the `.bazelrc` config to use (e.g., `debug`).

### Components

**1. Session orchestrator**

On launch:

1. Generates a UUID as the invocation ID.
2. Spawns the Bazel process using the configured `bazelBinary`:
   `<bazelBinary> test --config=<bazelConfig> --invocation_id=<id> <target>`
3. Connects to bb_debug_adapter via `bbStorageEndpoint` using the same
   invocation ID.
4. Waits for the worker to register before forwarding DAP messages. The
   `ReceiveMessages` stream blocks until the session is registered, so
   no polling is needed.

**2. DAP-to-gRPC bridge**

Implements VS Code's `DebugAdapter` interface:

- VS Code sends a DAP message → extension calls `SendMessages` on
  bb_debug_adapter.
- Extension calls `ReceiveMessages` (server-stream) → forwards DAP
  messages back to VS Code.
- Handles the DAP `initialize` / `launch` / `disconnect` lifecycle.

**3. Bazel process management**

The extension monitors the spawned Bazel child process:

- Bazel stdout/stderr is surfaced in VS Code's debug console so the user
  can see build progress and errors.
- If Bazel exits unexpectedly (build failure, crash), the extension
  surfaces the error and tears down the debug session.
- On normal debug session end (user clicks stop), the extension sends
  DAP `disconnect` through bb_debug_adapter and then terminates the
  Bazel process if it is still running.

### Implementation notes

- **Language**: TypeScript, using `@grpc/grpc-js` for the gRPC client.
- **Authentication**: If bb_storage requires auth, the extension needs
  additional credential fields in `launch.json` (TBD based on deployment).
- **Workspace settings**: `bazelBinary` and `bbStorageEndpoint` can be set
  at the workspace level in `.vscode/settings.json` to avoid repeating
  them in every `launch.json` entry.
