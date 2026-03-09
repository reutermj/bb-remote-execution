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

## Worker/Runner Modifications

- New platform property: `debug_adapter=<binary_name>` (e.g., `lldb-dap`,
  `debugpy`). The value names the DAP server binary to launch.
- Runner detects the `debug_adapter` platform property and launches the target
  process under the specified DAP server in stdin/stdout mode.
- Runner captures DAP server stdin/stdout and relays messages bidirectionally
  through bb_debug_adapter:
  - DAP server stdout -> `SendMessages` to bb_debug_adapter
  - `ReceiveMessages` from bb_debug_adapter -> DAP server stdin
- `Run()` blocks until the debug session ends (debugger disconnect or process
  exit).

## Session Lifecycle

1. Client submits action to Bazel with `debug_adapter=<binary>` platform
   property.
2. Scheduler assigns to worker normally.
3. Runner detects debug property, launches process under the specified DAP
   server, calls `RegisterSession`.
4. Runner streams DAP messages bidirectionally through bb_debug_adapter.
5. Client connects to bb_debug_adapter (via bb_storage frontend) using the
   invocation ID.
6. Client and worker exchange DAP messages through bb_debug_adapter.
7. On client disconnect or `EndSession`: bb_debug_adapter signals the worker,
   runner kills the DAP server, buffers are cleaned up.
8. On worker completion: buffered final messages are delivered to the client,
   session is cleaned up.

## Liveness Detection and Cleanup

### Detecting dead clients and workers

1. **Stream health**: Both `ReceiveMessages` streams carry periodic heartbeat
   keepalives. When a stream breaks, bb_debug_adapter detects the gRPC stream
   error immediately.
2. **Inactivity timeout**: If no messages flow in either direction for a
   configurable duration, bb_debug_adapter considers the session dead. This
   catches silent disconnects where neither side reconnects.

### Cleanup

- **Client dies**: bb_debug_adapter detects broken client stream or timeout.
  Sends a teardown signal on the worker's `ReceiveMessages` stream. Worker
  kills the DAP server process, `Run()` returns, action completes normally.
- **Worker dies**: bb_debug_adapter detects broken worker stream or timeout.
  Marks session as dead. Client's `ReceiveMessages` stream receives a
  termination error. Client knows the session is gone.
- **bb_debug_adapter restarts**: All in-memory session state is lost. Both
  sides detect broken streams. Worker kills the debug process (action fails).
  This is acceptable since debug sessions are inherently ephemeral.

## Constraints

- 1:1 mapping: one client, one worker, one debug session per invocation ID.
  No multi-client or multi-worker sessions.
- bb_debug_adapter does not interpret DAP messages — it is a pure relay.
- The DAP server must support stdin/stdout mode (most do).
