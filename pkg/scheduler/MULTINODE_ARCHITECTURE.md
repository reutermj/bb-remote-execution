# Multinode Execution Architecture

This document describes the planned architecture for multinode execution support in the `InMemoryBuildQueue`. Multinode execution allows a single action to be executed across multiple workers simultaneously, enabling distributed computations like MPI jobs.

## Code Flow: Execute with Sufficient Idle Workers

This walks through what happens when a client calls `Execute()` with `multinode_count=N` and there are at least N idle workers available.

**1. Client calls `Execute()` with an action containing `multinode_count=N`**

The request enters `Execute()` at [in_memory_build_queue.go:477](in_memory_build_queue.go#L477). The client provides an `ActionDigest` pointing to an `Action` message that includes a platform property `multinode_count=N`.

**2. Scheduler fetches the Action and extracts multinode_count**

At [line 504](in_memory_build_queue.go#L504), the scheduler fetches the `Action` message from CAS. Then at [line 512](in_memory_build_queue.go#L512), `processMultinodeCount()` is called:

```go
multinodeCount, action, err := bq.processMultinodeCount(action)
```

This function:
- Finds the `multinode_count` property in `Action.Platform.Properties`
- Validates the count (must be >= 1, <= `MaxMultinodeCount`)
- Returns a filtered action with `multinode_count` removed (since it's a scheduler directive, not a worker capability)

**3. Action is routed and platform queue is found**

After routing via `actionRouter.RouteAction()` at [line 538](in_memory_build_queue.go#L538), the scheduler looks up the platform queue at [line 581](in_memory_build_queue.go#L581):

```go
platformQueueIndex := bq.platformQueuesTrie.GetLongestPrefix(platformKey)
pq := bq.platformQueues[platformQueueIndex]
scq := pq.sizeClassQueues[sizeClassIndex]
```

**4. Scheduler creates a task group with N tasks**

At [line 609](in_memory_build_queue.go#L609), since `multinodeCount > 1`, a task group is created:

```go
group := &taskGroup{
    tasks:         make([]*task, multinodeCount),
    requiredCount: multinodeCount,
}
```

Then N tasks are created in a loop, each with:
- The same `actionDigest`
- A unique `MultinodeTaskIndex` (0 to N-1)
- A reference to the shared `group`

```go
for idx := 0; idx < multinodeCount; idx++ {
    t := &task{
        operations:   map[*invocation]*operation{},
        actionDigest: actionDigest,
        desiredState: remoteworker.DesiredState_Executing{
            // ... common fields ...
            MultinodeTaskIndex: int32(idx),
        },
        group: group,
    }
    group.tasks[idx] = t
}
```

Only the first task (index 0) is registered in `inFlightDeduplicationMap` for deduplication purposes.

**5. Scheduler checks for N idle synchronizing workers**

Before scheduling individual tasks, the scheduler must verify that N workers are available. This is a new check that differs from single-task scheduling:

```go
// Count available idle synchronizing workers in the size class queue
availableWorkers := countIdleSynchronizingWorkers(&scq.rootInvocation)
if availableWorkers < multinodeCount {
    // Not enough workers - queue all tasks (see different flow)
}
```

The count traverses the invocation tree, summing `len(i.idleSynchronizingWorkers)` at each level.

**6. Scheduler assigns all N tasks to workers atomically**

Since sufficient workers are available, all tasks are assigned in a single atomic operation (while holding the lock). For each task in the group:

```go
for idx, t := range group.tasks {
    // Find an idle worker (similar to single-task schedule())
    w := findAndDequeueIdleWorker(scq)

    t.registerQueuedStageStarted(bq, &scq.tasksScheduledWorker)
    w.assignUnqueuedTaskAndWakeUp(bq, t, 0)
}
```

Each call to `assignUnqueuedTaskAndWakeUp()` at [line 3059](in_memory_build_queue.go#L3059):
1. Closes the worker's `wakeup` channel via `wakeUp()`
2. Calls `assignUnqueuedTask()` to link the task and worker

**7. Workers wake up and receive their tasks**

Each worker was blocked in `getNextTask()` at the `select{}` statement around [line 3161](in_memory_build_queue.go#L3161). When its `wakeup` channel is closed, the `case <-wakeup:` branch executes:

```go
case <-wakeup:
    bq.enter(bq.clock.Now())
    if w.currentTask != nil {
        return w.getExecutingSynchronizeResponse(bq), nil
    }
```

Since the `Execute()` goroutine already set `w.currentTask`, each worker receives an executing response.

**8. Workers receive task details including peer information**

`getExecutingSynchronizeResponse()` at [line 3069](in_memory_build_queue.go#L3069) returns the `DesiredState_Executing` message. For multinode tasks, this includes:

```go
return &remoteworker.SynchronizeResponse{
    NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
    DesiredState: &remoteworker.DesiredState{
        WorkerState: &remoteworker.DesiredState_Executing_{
            Executing: &remoteworker.DesiredState_Executing{
                // ... standard fields ...
                MultinodeTaskIndex: t.desiredState.MultinodeTaskIndex,
                MultinodePeers:     group.getPeerInfo(),  // NEW: list of peer workers
            },
        },
    },
}
```

The `MultinodePeers` field contains information about all workers in the group, allowing them to coordinate (e.g., establish network connections for MPI).

**9. Client receives initial response and waits**

Back in `Execute()`, the client's operation calls `waitExecution()` at [line 649](in_memory_build_queue.go#L649):

```go
return firstOperation.waitExecution(bq, out)
```

The client receives an `Operation` message with `stage: EXECUTING` and blocks waiting for completion. The client only waits on the first task's operation - all tasks in the group share the same completion status.

**10. Workers execute and send heartbeats**

Each worker executes its portion of the multinode action. During execution, workers periodically call `Synchronize()` with `CurrentState_Executing`, which routes to `updateTask()` at [line 3229](in_memory_build_queue.go#L3229). This validates the worker is still running the correct task and returns the next synchronization time.

**Key points about this flow:**

- **Atomicity**: All N workers are assigned while holding the global lock, ensuring no partial assignments
- **No barrier needed**: Since all assignments happen atomically, workers can start executing immediately upon receiving their tasks
- **Peer discovery**: Workers learn about each other through the `MultinodePeers` field in the response
- **Single client wait**: The client waits on one operation; task group completion is handled separately (see completion flow)

## Code Flow: Execute with Insufficient Idle Workers

This walks through what happens when a client calls `Execute()` with `multinode_count=N` but fewer than N idle workers are currently available. The task group must be queued until enough workers become available.

**1. Client calls `Execute()` with an action containing `multinode_count=N`**

The request enters `Execute()` at [in_memory_build_queue.go:477](in_memory_build_queue.go#L477). The client provides an `ActionDigest` pointing to an `Action` message that includes a platform property `multinode_count=N`.

**2. Scheduler fetches the Action and extracts multinode_count**

At [line 504](in_memory_build_queue.go#L504), the scheduler fetches the `Action` message from CAS. Then at [line 512](in_memory_build_queue.go#L512), `processMultinodeCount()` is called:

```go
multinodeCount, action, err := bq.processMultinodeCount(action)
```

This function:
- Finds the `multinode_count` property in `Action.Platform.Properties`
- Validates the count (must be >= 1, <= `MaxMultinodeCount`)
- Returns a filtered action with `multinode_count` removed (since it's a scheduler directive, not a worker capability)

**3. Action is routed and platform queue is found**

After routing via `actionRouter.RouteAction()` at [line 538](in_memory_build_queue.go#L538), the scheduler looks up the platform queue at [line 581](in_memory_build_queue.go#L581):

```go
platformQueueIndex := bq.platformQueuesTrie.GetLongestPrefix(platformKey)
pq := bq.platformQueues[platformQueueIndex]
scq := pq.sizeClassQueues[sizeClassIndex]
```

**4. Scheduler creates a task group with N tasks**

At [line 609](in_memory_build_queue.go#L609), since `multinodeCount > 1`, a task group is created:

```go
group := &taskGroup{
    tasks:         make([]*task, multinodeCount),
    requiredCount: multinodeCount,
}
```

Then N tasks are created in a loop, each with:
- The same `actionDigest`
- A unique `MultinodeTaskIndex` (0 to N-1)
- A reference to the shared `group`

```go
for idx := 0; idx < multinodeCount; idx++ {
    t := &task{
        operations:   map[*invocation]*operation{},
        actionDigest: actionDigest,
        desiredState: remoteworker.DesiredState_Executing{
            // ... common fields ...
            MultinodeTaskIndex: int32(idx),
        },
        group: group,
    }
    group.tasks[idx] = t
}
```

Only the first task (index 0) is registered in `inFlightDeduplicationMap` for deduplication purposes.

**5. Scheduler checks for N idle synchronizing workers**

Before scheduling individual tasks, the scheduler must verify that N workers are available:

```go
// Count available idle synchronizing workers in the size class queue
availableWorkers := countIdleSynchronizingWorkers(&scq.rootInvocation)
if availableWorkers < multinodeCount {
    // Not enough workers - must queue the task group
}
```

The count traverses the invocation tree, summing `len(i.idleSynchronizingWorkers)` at each level. In this flow, `availableWorkers < multinodeCount`, so we cannot assign immediately.

**6. Scheduler queues the task group via the lead task's operation**

Since there aren't enough workers, the task group must be queued. To reuse the existing `queuedOperations` heap structure, only the **lead task** (task 0) has its operation enqueued. The other N-1 tasks exist but are not individually queued—they are accessed through `group.tasks` when the group is assigned.

```go
// Only enqueue the lead task's operation to represent the entire group
leadTask := group.tasks[0]
leadTask.registerQueuedStageStarted(bq, &scq.tasksScheduledQueue)
for _, o := range leadTask.operations {
    o.enqueue()
}

// Mark other tasks as queued for metrics, but don't enqueue their operations
for i := 1; i < len(group.tasks); i++ {
    group.tasks[i].registerQueuedStageStarted(bq, &scq.tasksScheduledQueue)
}
```

The `enqueue()` call at [line 2362](in_memory_build_queue.go#L2362) adds the lead operation to the invocation's priority queue:

```go
func (o *operation) enqueue() {
    i := o.invocation
    i.maybeActivate()
    heap.Push(&i.queuedOperations, o)
    // Update parent invocations
    for i.parent != nil {
        i.updateFirstOperationPriority()
        i.parent.maybeActivate()
        heapPushOrFix(&i.parent.queuedChildren, i.queuedChildrenIndex, i)
        i = i.parent
    }
}
```

The task group remains in the QUEUED stage. The group's priority and expected duration are determined by the lead task's operation, which represents the entire group in the queue.

**7. Client receives initial response and waits**

Back in `Execute()`, the client's operation calls `waitExecution()` at [line 649](in_memory_build_queue.go#L649):

```go
return firstOperation.waitExecution(bq, out)
```

The client receives an `Operation` message with `stage: QUEUED` and blocks waiting for updates. The `waitExecution()` loop at [line 2272](in_memory_build_queue.go#L2272) periodically sends status updates:

```go
for {
    metadata, err := anypb.New(&remoteexecution.ExecuteOperationMetadata{
        Stage:        t.getStage(),  // QUEUED
        ActionDigest: t.desiredState.ActionDigest,
    })
    // ... send to client ...

    select {
    case <-ctx.Done():
        // Client disconnected
    case <-stageChangeWakeup:
        // Stage changed (e.g., to EXECUTING)
    case t := <-timerChannel:
        // Periodic update
    }
}
```

**8. Workers become available and call Synchronize(Idle)**

Over time, workers finish their current tasks or new workers connect. When a worker calls `Synchronize()` with `CurrentState_Idle`, it enters `getCurrentOrNextTask()` at [line 3192](in_memory_build_queue.go#L3192), which calls `getNextTask()` at [line 3086](in_memory_build_queue.go#L3086).

**9. Worker attempts to dequeue a task via `assignNextQueuedTask()`**

In `getNextTask()`, the worker calls `assignNextQueuedTask()` to find work:

```go
func (w *worker) getNextTask(...) (*remoteworker.SynchronizeResponse, error) {
    // ... preferBeingIdle check ...

    if !isDrained && w.assignNextQueuedTask(bq, scq, workerID) {
        return w.getExecutingSynchronizeResponse(bq), nil
    }
    // ... rest of existing logic (queue self and wait) ...
}
```

The `assignNextQueuedTask()` function at [line 2956](in_memory_build_queue.go#L2956) traverses the invocation tree looking for the highest-priority queued operation.

**10. Worker encounters a multinode task group in the queue**

When `assignNextQueuedTask()` finds a queued operation, it checks if the operation's task belongs to a group:

```go
func (w *worker) assignNextQueuedTask(bq *InMemoryBuildQueue, scq *sizeClassQueue, workerID map[string]string) bool {
    // ... existing invocation traversal logic ...

    for {
        if len(i.queuedOperations) > 0 {
            o := i.queuedOperations[0]
            t := o.task

            // Check if this task is part of a multinode group
            if group := t.group; group != nil {
                // Count available idle synchronizing workers (+1 for this worker)
                availableWorkers := countIdleSynchronizingWorkers(&scq.rootInvocation) + 1

                if availableWorkers < group.requiredCount {
                    // Head-of-line block: not enough workers to assign this group.
                    // Return false rather than skipping to lower-priority tasks.
                    // This prevents starvation of multinode jobs—without this,
                    // a steady stream of single-node jobs could continuously
                    // "jump the queue" ahead of multinode jobs.
                    return false
                }

                // Enough workers available - assign the entire group
                return w.assignTaskGroup(bq, scq, group, workerID)
            }

            // Single task - assign normally
            w.assignQueuedTask(bq, t, stickinessRetained)
            return true
        }
        // ... continue traversing queuedChildren ...
    }
}
```

The trade-off of head-of-line blocking is that workers may sit idle even when single-node work is available, but this ensures fair scheduling based on priority rather than favoring jobs that happen to need fewer resources.

**11. Scheduler assigns all N tasks to workers atomically**

When enough workers are available, `assignTaskGroup()` assigns all N tasks while holding the lock:

```go
func (w *worker) assignTaskGroup(bq *InMemoryBuildQueue, scq *sizeClassQueue, group *taskGroup, workerID map[string]string) bool {
    // Collect N workers: this worker + N-1 idle synchronizing workers
    workers := make([]*worker, group.requiredCount)
    workers[0] = w

    for i := 1; i < group.requiredCount; i++ {
        // Find and dequeue an idle synchronizing worker from the invocation tree
        otherWorker := findAndDequeueIdleWorker(scq)
        workers[i] = otherWorker
    }

    // Remove the lead task's operation from the queue
    leadOp := group.tasks[0].operations[/* invocation */]
    leadOp.removeQueuedFromInvocation()

    // Assign each task to its worker
    for i, t := range group.tasks {
        t.registerQueuedStageFinished(bq)

        if i == 0 {
            // Lead task assigned to triggering worker (already dequeued above)
            w.assignUnqueuedTask(bq, t, 0)
        } else {
            // Other tasks assigned to idle workers, wake them up
            workers[i].wakeUp(scq)
            workers[i].assignUnqueuedTask(bq, t, 0)
        }
    }

    return true
}
```

The triggering worker returns `true` and proceeds to `getExecutingSynchronizeResponse()`. The other N-1 workers were blocked in `getNextTask()` at the `select{}` and wake up when their `wakeup` channel is closed.

**12. All workers receive their tasks**

The triggering worker (worker 0) receives its response after `assignTaskGroup()` returns `true`, leading to `getExecutingSynchronizeResponse()`. The other N-1 workers were blocked in `getNextTask()` at the `select{}`:

```go
select {
case <-wakeup:
    bq.enter(bq.clock.Now())
    if w.currentTask != nil {
        return w.getExecutingSynchronizeResponse(bq), nil
    }
}
```

Since `assignQueuedTask()` set `w.currentTask` before closing `wakeup`, each worker receives an executing response.

**13. Workers receive task details including peer information**

`getExecutingSynchronizeResponse()` at [line 3069](in_memory_build_queue.go#L3069) returns the `DesiredState_Executing` message. For multinode tasks, this includes:

```go
return &remoteworker.SynchronizeResponse{
    NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
    DesiredState: &remoteworker.DesiredState{
        WorkerState: &remoteworker.DesiredState_Executing_{
            Executing: &remoteworker.DesiredState_Executing{
                // ... standard fields ...
                MultinodeTaskIndex: t.desiredState.MultinodeTaskIndex,
                MultinodePeers:     group.getPeerInfo(),  // List of all peer workers
            },
        },
    },
}
```

**14. Client is notified of stage change**

When the first task transitions from QUEUED to EXECUTING, `reportNonFinalStageChange()` is called at [line 2951](in_memory_build_queue.go#L2951):

```go
func (t *task) reportNonFinalStageChange() {
    close(t.stageChangeWakeup)
    t.stageChangeWakeup = make(chan struct{})
}
```

This wakes up the client blocked in `waitExecution()`. On the next loop iteration, the client receives an `Operation` with `stage: EXECUTING`.

**15. Workers execute and send heartbeats**

Each worker executes its portion of the multinode action. During execution, workers periodically call `Synchronize()` with `CurrentState_Executing`, which routes to `updateTask()` at [line 3229](in_memory_build_queue.go#L3229). This validates the worker is still running the correct task and returns the next synchronization time.

**Key points about this flow:**

- **Single queue entry**: Only the lead task's operation is enqueued in `queuedOperations`; the other N-1 tasks are accessed via `group.tasks`
- **Existing queue structure**: Reuses the existing `queuedOperations` heap—no separate queue for task groups
- **Group check in `assignNextQueuedTask()`**: When dequeuing an operation, check if `task.group != nil` and verify worker availability before assignment
- **Deferred assignment**: The group waits in the queue until N workers are simultaneously available
- **Triggering worker**: The worker whose arrival makes the group assignable calls `assignTaskGroup()` to assign all N tasks
- **Atomic assignment**: All N workers are assigned while holding the global lock
- **Head-of-line blocking**: When a multinode group can't be assigned, we block rather than skip to lower-priority single tasks—this prevents starvation of multinode jobs at the cost of potentially idle workers
