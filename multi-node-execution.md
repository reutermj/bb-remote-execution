# Multi-Node Execution Design

This document describes the design for adding multi-node execution support to bb-scheduler, enabling Bazel tests that require coordination across N workers running the same binary.

## Development

Build and test using `./bazel` (the wrapper script in the repo root).

## Overview

Multi-node execution allows a single Bazel test to be executed across N workers simultaneously. All workers run the same binary and are provided with peer IP addresses via environment variables to enable coordination. The test framework handles rank election internally.

## User Interface

### Bazel Configuration

Tests specify multi-node requirements via `exec_properties`:

```python
cc_test(
    name = "distributed_test",
    srcs = ["distributed_test.cc"],
    exec_properties = {
        "multinode.count": "4",
    },
)
```

### Worker Configuration

Workers automatically detect and include their IP address in the `WorkerId` map. The IP is determined by checking which local address would be used to reach an external endpoint (e.g., `8.8.8.8`), which works correctly even behind NAT.

The auto-detected IP can be overridden in the runner configuration if needed:

```jsonnet
{
  runners: [{
    // ... other runner config ...

    // Optional: Override auto-detected IP address
    advertise_ip: "10.0.1.15",
  }],
}
```

**IP resolution priority:**
1. `advertise_ip` config field (if set)
2. `worker_id["ip"]` from config (legacy, for backwards compatibility)
3. Auto-detected outbound IP address

### Environment Variables

Each worker receives this environment variable:

| Variable | Description | Example |
|----------|-------------|---------|
| `MULTINODE_PEERS` | Comma-separated list of all peer IPs (ordered by index) | `10.0.1.15,10.0.1.16,10.0.1.17,10.0.1.18` |

The test framework can derive the count from the list length and determine its own index by matching its IP against the list.

## Architecture

### New Types

#### TaskGroup

A `taskGroup` coordinates N tasks that must execute together:

```go
type taskGroup struct {
    tasks           []*task
    requiredCount   int
    assignedWorkers []*worker
    allAssigned     bool

    // Completion tracking
    completedCount  int
    failed          bool
    firstError      *remoteexecution.ExecuteResponse

    // Retry applies to the whole group, not individual tasks.
    retryCount int
}
```

#### Task Extensions

The existing `task` struct gains optional group membership:

```go
type task struct {
    // ... existing fields ...

    // Multi-node support
    group *taskGroup  // nil for single-node tasks
}
```

### Scheduling Flow

#### 1. Execute() - Task Group Creation

When `Execute()` receives an action with `multinode.count > 1`:

1. Parse `multinode.count` from `action.Platform.Properties`
2. Create a `taskGroup` with N `task` objects
3. All tasks share the same `actionDigest` and base `desiredState`
4. Schedule all N tasks (they will be queued until N workers are available)

#### 2. schedule() - Priority-Respecting Barrier

Multi-node tasks respect queue priority. When `schedule()` is called on a grouped task:

1. Queue the task normally (grouped tasks are always queued, never immediately assigned)
2. When a worker becomes idle and looks for work, it checks the head of the queue
3. If the head task is a multi-node group, workers wait until N workers are idle
4. Once N workers are idle, `assignAllWorkers()` assigns all tasks atomically
5. If the head task is a single-node task, assign it normally

This approach ensures multi-node tasks are not starved by a continuous stream of single-node tasks.

#### 3. Size Class Selection

Multi-node tasks always use the largest size class to ensure consistent worker capabilities across all nodes. Size class learning is not applied to multi-node executions.

#### 4. Worker Assignment with Peer IPs

When all N workers are available, `assignAllWorkers()`:

1. Collects N idle workers from the size class queue
2. Extracts IP addresses from each worker's `WorkerId` map
3. Sets `AdditionalEnvironmentVariables` on each task's `desiredState` with `MULTINODE_PEERS`
4. Assigns each task to its worker via `assignUnqueuedTaskAndWakeUp()`

### Completion Handling

#### Any Failure = Group Failure

When any task in a group completes with a failure (non-OK status or non-zero exit code):

1. Mark the group as failed and save the first error
2. Cancel all other executing tasks in the group
3. When all tasks complete, either retry or finalize

#### Result Aggregation

The group returns a single `ExecuteResponse` to the client:
- On failure: returns the first error
- On success: returns the first task's response

### Retry Semantics

Retries apply to the entire group, not individual nodes. When any node fails:

1. All other executing tasks are cancelled
2. If retries remain (`retryCount < WorkerTaskRetryCount`), reset group state and re-queue all tasks
3. Fresh workers are assigned with new peer IP addresses
4. Otherwise, finalize with the error

### Deduplication

Multi-node tasks use in-flight deduplication like single-node tasks. The first task in the group is registered in `inFlightDeduplicationMap`, and duplicate requests attach to the existing task group.

## Queueing Behavior

When fewer than N workers are available:

1. Tasks remain in QUEUED state
2. If a multi-node task is at the head of the queue, idle workers wait (no work assigned)
3. Single-node tasks behind the multi-node task also wait (priority is respected)
4. Once N workers are idle, the multi-node group is assigned atomically
5. After the multi-node task is assigned, normal queue processing resumes

## Configuration

New configuration option for `InMemoryBuildQueueConfiguration`:

```go
type InMemoryBuildQueueConfiguration struct {
    // ... existing fields ...

    // MaxMultinodeCount limits the maximum number of nodes in a multi-node
    // execution group. Requests exceeding this are rejected.
    // Default: 16
    MaxMultinodeCount int
}
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| `multinode.count` > `MaxMultinodeCount` | Rejected with `InvalidArgument` |
| Worker missing `ip` in WorkerId | Rejected with `FailedPrecondition` |
| Node crashes mid-execution | Group fails, other nodes cancelled, retry if eligible |
| Timeout during execution | All nodes cancelled, group fails with `DeadlineExceeded` |

Note: There is no scheduler-side barrier timeout. Queued tasks wait indefinitely for workers. Clients (e.g., Bazel with `--remote_timeout`) are expected to enforce their own timeouts.

## Future Considerations

### Not in Scope (v1)

- **Rank election**: Handled by test framework, not scheduler
- **Worker-to-worker communication**: Tests use peer IPs directly
- **Heterogeneous node types**: All nodes must match same platform
- **Partial success**: Any failure = total failure
- **Log merging**: Logs stored separately, not merged by scheduler

### Potential Future Enhancements

- BuildQueueState API to query per-node status and logs
- Metrics for multi-node queue wait times and success rates
- Support for "soft" node counts (run with N-1 if one fails to allocate)
