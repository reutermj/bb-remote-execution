# In-Memory Build Queue Architecture

This document describes the architecture of the `InMemoryBuildQueue`, the core scheduler component that matches build tasks from clients with available workers.

## Platform Queues

The scheduler routes actions to workers based on **instance name** and **platform**.

### Instance Name

The instance name is a namespace from the Remote Execution API. It allows a single scheduler to serve multiple isolated environments:

- `""` (empty) — default instance
- `"ci"` — CI builds
- `"production"` — production builds
- `"team-a/project-x"` — hierarchical namespaces

### Platform

The platform specifies worker requirements from the `Action.Platform` field:

- OS (e.g., `os:linux`, `os:windows`)
- Architecture (e.g., `arch:x86_64`, `arch:arm64`)
- Custom properties (e.g., `docker-image:ubuntu:22.04`, `gpu:true`)

### Routing

The scheduler combines instance name + platform into a `platformKey` to find the right worker pool:

```
platformKey = instanceName + platform

Examples:
  "" + "os:linux,arch:x86_64"      → default linux-x86_64 workers
  "ci" + "os:linux,arch:x86_64"    → CI linux-x86_64 workers
  "ci" + "os:linux,arch:arm64"     → CI linux-arm64 workers
```

At [line 513](in_memory_build_queue.go#L513), the scheduler looks up the platform queue:

```go
platformQueueIndex := bq.platformQueuesTrie.GetLongestPrefix(platformKey)
if platformQueueIndex < 0 {
    return status.Errorf(code, "No workers exist for instance name prefix %#v platform %s", ...)
}
pq := bq.platformQueues[platformQueueIndex]
```

The trie uses **longest prefix matching**, so a worker registered for `"ci"` can handle actions for `"ci/team-a"` if no more specific workers exist.

### Data Hierarchy

```
InMemoryBuildQueue
  └── platformQueue (one per instance name + platform)
        ├── sizeClassQueues (worker pools by size/capability)
        │     ├── workers
        │     └── rootInvocation
        │           └── invocations (client build sessions)
        │                 └── queuedOperations
        └── sizeClasses, workerInvocationStickinessLimits, etc.
```

Each `platformQueue` contains one or more `sizeClassQueue` entries.

### Size Classes

Size classes partition workers within a platform by capability (e.g., CPU cores, memory). This allows:

- **Resource matching**: Small actions run on small workers, large actions on large workers
- **Cost optimization**: Avoid wasting large workers on small tasks
- **Failure recovery**: If an action fails on a small worker, retry on a larger one

Workers register with a size class (a `uint32` value). Actions are assigned to a size class by the `initialSizeClassSelector` at [line 532](in_memory_build_queue.go#L532):

```go
sizeClassIndex, expectedDuration, timeout, initialSizeClassLearner := initialSizeClassSelector.Select(pq.sizeClasses)
scq := pq.sizeClassQueues[sizeClassIndex]
```

**Size class learning**: If a task fails (especially with `DeadlineExceeded`), the scheduler can retry it on a larger size class. At [line 2591](in_memory_build_queue.go#L2591):

```go
if t.initialSizeClassLearner != nil {
    // Re-execution against the largest size class is requested.
    // Transplant all operations to the other size class queue and reschedule.
    largestSCQ := pq.sizeClassQueues[len(pq.sizeClassQueues)-1]
    // ... move operations to largestSCQ ...
    t.schedule(bq)
}
```

This allows the system to learn which actions need larger workers without manual configuration.

### Size Class Learning Persistence

The scheduler supports two modes for size class selection:

1. **Fallback Analyzer** (default): Always starts actions on the smallest size class. No learning is persisted — each build starts fresh.

2. **Feedback-Driven Analyzer** (with ISCC): Uses the **Initial Size Class Cache (ISCC)**, a persistent store that remembers execution outcomes keyed by action digest.

When Feedback-Driven mode is enabled:

**What is stored**:
- Per-size-class execution history (success times, failures, timeouts)
- `LastSeenFailure` timestamp — when this action last failed

**How persistence works**:
1. When `Execute()` is called, the analyzer reads previous stats from ISCC via `reducedActionDigest`
2. If `LastSeenFailure` is within `failureCacheDuration`, the action skips smaller size classes entirely
3. After execution completes, the `Learner` writes updated stats back to ISCC

**Example flow**:
```
Build 1: Action A runs on small worker → fails
         → Learner.Failed() called
         → LastSeenFailure set to now
         → Stats written to ISCC

Build 2: Action A requested again
         → Analyzer.Analyze() reads ISCC
         → Sees LastSeenFailure < failureCacheDuration ago
         → Routes directly to largest size class
```

The history size is configurable — older executions are evicted as new ones are recorded, allowing the system to adapt if an action's resource requirements change over time.

## Locking

The scheduler uses a **single global lock** (`sync.Mutex`) to protect all state. Every operation (`Execute`, `Synchronize`, `GetOperation`, etc.) acquires this lock via `bq.enter()`/`bq.leave()` helpers.

The one exception: when a worker blocks in `select{}` waiting for work, it releases the lock so other goroutines can proceed, then reacquires when woken up.

This design trades potential contention under high load for simpler code with no deadlock or lock-ordering risks.

## Worker-Scheduler Connection

Workers connect to the scheduler using a **long-polling** pattern via the `Synchronize()` gRPC method.

### Code Flow: Worker Registration (First Synchronize)

When a worker calls `Synchronize()` for the first time, the scheduler creates data structures to track it.

**1. Worker sends first `Synchronize()` request**

The request includes `InstanceNamePrefix`, `Platform`, `SizeClass`, and `WorkerId`. The scheduler builds keys at [line 617](in_memory_build_queue.go#L617):

```go
sizeClassKey := sizeClassKey{
    platformKey: platformKey,
    sizeClass:   request.SizeClass,
}
```

**2. Platform queue lookup/creation**

At [line 622](in_memory_build_queue.go#L622), the scheduler looks for an existing size class queue:

- **If found**: Use existing queue, cancel any pending cleanup
- **If not found, but platform exists**: Create new size class queue for this size class (if predeclared)
- **If platform not found**: Create new platform queue and size class queue at [line 653](in_memory_build_queue.go#L653):

```go
pq = bq.addPlatformQueue(platformKey, nil, 0, 0)
scq = pq.addSizeClassQueue(bq, request.SizeClass, true)
```

**3. Worker creation**

At [line 666](in_memory_build_queue.go#L666), since this worker hasn't been seen before, a new worker struct is created:

```go
i := &scq.rootInvocation
w = &worker{
    workerKey:               workerKey,
    lastInvocation:          i,      // Start at root invocation
    listIndex:               -1,     // Not queued yet
    stickinessStartingTimes: make([]time.Time, len(pq.workerInvocationStickinessLimits)),
}
i.idleWorkersCount++
scq.workers[workerKey] = w
```

The worker is associated with the **root invocation** — it has no locality preference yet.

**4. Worker proceeds to get work**

The new worker continues to `getCurrentOrNextTask()` and either gets a task or blocks waiting (see subsequent flows).

**5. Cleanup callback scheduled**

At the end of `Synchronize()`, a cleanup callback is scheduled. If the worker never calls `Synchronize()` again, it will be removed.

### Code Flow: Idle Worker, No Jobs Available, Worker Times Out

This walks through what happens when a worker calls `Synchronize(Idle)` and there are no tasks in the queue.

**1. Worker sends `Synchronize()` with `CurrentState_Idle`**

The request enters `Synchronize()` at [in_memory_build_queue.go:597](in_memory_build_queue.go#L597). After validation, it switches on the worker state at [line 697](in_memory_build_queue.go#L697):

```go
case *remoteworker.CurrentState_Idle:
    return w.getCurrentOrNextTask(ctx, bq, scq, request.WorkerId, request.PreferBeingIdle)
```

**2. `getCurrentOrNextTask()` finds no current task**

At [line 3057](in_memory_build_queue.go#L3057), `w.currentTask` is nil (worker has no assigned task), so we skip the retry logic and call:

```go
return w.getNextTask(ctx, bq, scq, workerID, preferBeingIdle)
```

**3. `getNextTask()` tries to find queued work**

At [line 2952](in_memory_build_queue.go#L2952), the scheduler attempts to assign a queued task:

```go
if !isDrained && w.assignNextQueuedTask(bq, scq, workerID) {
    return w.getExecutingSynchronizeResponse(bq), nil
}
```

But there are no tasks, so `assignNextQueuedTask()` returns false.

**4. Worker queues itself and blocks**

Since no work is available, the worker adds itself to `idleSynchronizingWorkers` and blocks. At [line 2993](in_memory_build_queue.go#L2993):

```go
wakeup := make(chan struct{})
w.wakeup = wakeup
i := w.lastInvocation
i.idleSynchronizingWorkers.enqueue(...)

bq.leave()  // Release the lock

select {
case t := <-timeoutChannel:
    // Health check timeout
case <-ctx.Done():
    // Worker disconnected
case <-wakeup:
    // Task was assigned
}
```

The gRPC call remains open while blocking on this `select{}`.

**5. Timeout fires, worker told to resync**

After `GetIdleWorkerSynchronizationInterval` passes, the timeout case executes. The worker is dequeued and receives an idle response at [line 3026](in_memory_build_queue.go#L3026):

```go
return bq.getIdleSynchronizeResponse(), nil
```

This returns `DesiredState: Idle` with `NextSynchronizationAt: now`, telling the worker to stay idle and resync immediately.

**6. Worker loops**

The worker receives the response, sees it should stay idle, and immediately calls `Synchronize(Idle)` again. The cycle repeats.

### Code Flow: Idle Worker, No Jobs Available, Job Arrives Before Timeout

This continues from the previous flow. The worker is blocked in `select{}` (step 4 above). Meanwhile, the `Execute()` flow (see below) assigns a task to this worker and closes its `wakeup` channel.

**5. Worker's `select{}` unblocks**

The closed `wakeup` channel causes the `case <-wakeup:` branch to execute at [line 3032](in_memory_build_queue.go#L3032):

```go
case <-wakeup:
    bq.enter(bq.clock.Now())
    if w.currentTask != nil {
        return w.getExecutingSynchronizeResponse(bq), nil
    }
```

The `Execute()` goroutine already set `w.currentTask`, so the worker receives an executing response.

**6. Worker receives task and executes**

`getExecutingSynchronizeResponse()` at [line 2926](in_memory_build_queue.go#L2926) returns:

```go
return &remoteworker.SynchronizeResponse{
    NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
    DesiredState: &remoteworker.DesiredState{
        WorkerState: &remoteworker.DesiredState_Executing_{
            Executing: &t.desiredState,
        },
    },
}
```

The worker receives `DesiredState: Executing` with the task details and begins execution.

### Code Flow: Idle Worker, Jobs Queued

This walks through what happens when a worker calls `Synchronize(Idle)` and there are tasks waiting in the queue.

**1. Worker sends `Synchronize()` with `CurrentState_Idle`**

Same as the "No Jobs Available" flow — the request enters `Synchronize()` and calls `getCurrentOrNextTask()`.

**2. `getCurrentOrNextTask()` calls `getNextTask()`**

At [line 3057](in_memory_build_queue.go#L3057), `w.currentTask` is nil, so we call `getNextTask()`.

**3. `getNextTask()` finds queued work**

At [line 2952](in_memory_build_queue.go#L2952), the scheduler attempts to assign a queued task:

```go
if !isDrained && w.assignNextQueuedTask(bq, scq, workerID) {
    return w.getExecutingSynchronizeResponse(bq), nil
}
```

This time `assignNextQueuedTask()` returns true because there are queued operations.

**4. `assignNextQueuedTask()` traverses the invocation tree**

At [line 2813](in_memory_build_queue.go#L2813), the worker searches for the best queued operation:

```go
func (w *worker) assignNextQueuedTask(...) bool {
    i := &scq.rootInvocation
    for {
        if len(i.queuedOperations) > 0 {
            // Pick the most preferable operation
            w.assignQueuedTask(bq, i.queuedOperations[0].task, stickinessRetained)
            return true
        } else if len(i.queuedChildren) > 0 {
            // Descend into child invocation with queued work
            i = i.queuedChildren[0]
        } else {
            return false
        }
    }
}
```

**5. Task is dequeued and assigned**

`assignQueuedTask()` removes the operation from the queue and assigns it to the worker. The worker immediately receives an executing response — no blocking needed.

**Key point**: This is the pull model. The worker finds and assigns the task to itself, unlike the "Job Arrives Before Timeout" flow where the `Execute()` goroutine pushes the task to an idle worker.

### Code Flow: Worker Executing Task (Heartbeat)

While executing a task, workers periodically call `Synchronize()` to report progress and confirm liveness.

**1. Worker sends `Synchronize()` with `CurrentState_Executing`**

During execution, the worker reports its current execution state:

```
execution_state (oneof):
  - started           → initializing build environment
  - fetching_inputs   → downloading input files
  - running           → command is executing
  - uploading_outputs → uploading results
  - completed         → done (see Task Completion Flow)
```

All states except `completed` route to `updateTask()` at [line 709](in_memory_build_queue.go#L709):

```go
default:
    return w.updateTask(bq, scq, request.WorkerId, executing.ActionDigest, request.PreferBeingIdle)
```

**2. Scheduler validates and acknowledges**

At [line 3088](in_memory_build_queue.go#L3088), the scheduler validates the worker is still working on the correct task:

```go
func (w *worker) updateTask(...) (*remoteworker.SynchronizeResponse, error) {
    if !w.isRunningCorrectTask(actionDigest) {
        return w.getCurrentOrNextTask(nil, bq, scq, workerID, preferBeingIdle)
    }
    // The worker is doing fine. Allow it to continue
    return &remoteworker.SynchronizeResponse{
        NextSynchronizationAt: bq.getNextSynchronizationAtDelay(),
    }, nil
}
```

**3. Worker receives next sync time**

The response contains `NextSynchronizationAt` set to `now + BusyWorkerSynchronizationInterval`. The worker should call `Synchronize()` again around this time.

**4. Cycle repeats until completion**

The worker continues sending heartbeats until execution finishes, then sends `completed` state (see Task Completion Flow).

**Why heartbeats matter:**
- **Liveness detection**: If heartbeats stop, `WorkerWithNoSynchronizationsTimeout` fires and the task is failed (see next flow)
- **Action digest validation**: If the worker reports the wrong action digest, it's redirected to `getCurrentOrNextTask()`
- **Future extensibility**: The scheduler could use heartbeats to cancel tasks or adjust priorities

### Code Flow: Worker Stops Heartbeating (Stale Worker)

When a worker stops calling `Synchronize()` while executing a task, the scheduler detects this and fails the task.

**1. Worker's last `Synchronize()` schedules cleanup**

At the end of every `Synchronize()` call, a cleanup callback is scheduled at [line 686](in_memory_build_queue.go#L686):

```go
defer func() {
    removalTime := bq.now.Add(bq.configuration.WorkerWithNoSynchronizationsTimeout)
    bq.cleanupQueue.add(&w.cleanupKey, removalTime, func() {
        scq.removeStaleWorker(bq, workerKey, removalTime)
    })
}()
```

If the worker calls `Synchronize()` again, the old callback is removed and a new one is scheduled (line 665).

**2. Worker stops sending heartbeats**

The worker crashes, loses network, or is killed. No more `Synchronize()` calls arrive.

**3. Timeout fires**

After `WorkerWithNoSynchronizationsTimeout`, the cleanup callback fires and calls `removeStaleWorker()` at [line 1577](in_memory_build_queue.go#L1577).

**4. Task is completed with an error**

Since `w.currentTask != nil`, the task is failed:

```go
t.complete(bq, &remoteexecution.ExecuteResponse{
    Status: status.Newf(codes.Unavailable, "Worker %s disappeared while task was executing", workerKey).Proto(),
}, false)
```

**5. Clients receive the error**

The `completedByWorker = false` argument means:
- No size class learning is recorded (`initialSizeClassLearner.Abandoned()`)
- The task is **not** retried on a larger size class
- Clients receive the `Unavailable` error immediately

**6. Worker is removed**

The worker is deleted from scheduler state and can re-register on next `Synchronize()`.

### Code Flow: Drained Worker

Draining allows operators to gracefully remove workers without interrupting running tasks. Drained workers finish their current work but don't receive new tasks.

**Adding a drain:**

1. Operator calls `AddDrain()` with a worker ID pattern at [line 1191](in_memory_build_queue.go#L1191)
2. The drain is stored in `scq.drains` map
3. Any idle workers matching the pattern that are currently blocked waiting for work are woken up:

```go
for workerKey, w := range scq.workers {
    if w.wakeup != nil && workerMatchesPattern(workerKey.getWorkerID(), request.WorkerIdPattern) {
        w.wakeUp(scq)
    }
}
```

**What happens when a drained worker calls `Synchronize(Idle)`:**

1. Worker calls `Synchronize()` and reaches `getNextTask()`
2. At [line 2951](in_memory_build_queue.go#L2951), `isDrained` is checked:

```go
isDrained := w.isDrained(scq, workerID)
if !isDrained && w.assignNextQueuedTask(bq, scq, workerID) {
    return w.getExecutingSynchronizeResponse(bq), nil
}
```

3. Since `isDrained` is true, the worker skips task assignment and enters the drain wait loop at [line 2969](in_memory_build_queue.go#L2969):

```go
if isDrained {
    undrainWakeup := scq.undrainWakeup
    bq.leave()

    select {
    case t := <-timeoutChannel:
        return bq.getIdleSynchronizeResponse(), nil
    case <-ctx.Done():
        return nil, util.StatusFromContext(ctx)
    case <-undrainWakeup:
        // Worker might have been undrained
        bq.enter(bq.clock.Now())
    }
}
```

4. The worker blocks on `undrainWakeup` channel instead of getting tasks
5. After timeout, worker receives idle response and re-syncs (still drained)

**Removing a drain:**

1. Operator calls `RemoveDrain()` at [line 1211](in_memory_build_queue.go#L1211)
2. The drain is deleted from the map
3. `undrainWakeup` channel is closed, waking all drained workers:

```go
close(scq.undrainWakeup)
scq.undrainWakeup = make(chan struct{})
```

4. Workers wake up, re-check `isDrained`, and can now receive tasks

**Note**: Workers executing tasks are not affected by drains — they complete their current task normally. The drain only prevents new task assignment.

### Code Flow: Task Delivery Failure (Retry)

When the scheduler assigns a task to a worker but the worker reports idle on its next sync, the task delivery failed. This can happen due to network issues losing the response, or the worker crashing and restarting.

**1. Scheduler assigns task to worker**

Via `schedule()` or `assignNextQueuedTask()`, the task is assigned: `w.currentTask = t`.

**2. Worker reports idle (delivery failed)**

The worker calls `Synchronize(Idle)`. At [line 697](in_memory_build_queue.go#L697):

```go
case *remoteworker.CurrentState_Idle:
    return w.getCurrentOrNextTask(ctx, bq, scq, request.WorkerId, request.PreferBeingIdle)
```

**3. `getCurrentOrNextTask()` detects mismatch**

At [line 3050](in_memory_build_queue.go#L3050), the scheduler sees `w.currentTask != nil` but the worker reported idle:

```go
if t := w.currentTask; t != nil {
    if t.retryCount < bq.configuration.WorkerTaskRetryCount {
        t.retryCount++
        return &remoteworker.SynchronizeResponse{
            DesiredState: &remoteworker.DesiredState{
                WorkerState: &remoteworker.DesiredState_Executing_{
                    Executing: &t.desiredState,
                },
            },
        }, nil
    }
```

**4a. Retries available: re-send task**

If `retryCount < WorkerTaskRetryCount`, the task is re-sent. The worker receives the same `DesiredState_Executing` and should execute it.

**4b. Retries exhausted: fail task**

At [line 3062](in_memory_build_queue.go#L3062), if too many retries have failed:

```go
t.complete(bq, &remoteexecution.ExecuteResponse{
    Status: status.Newf(
        codes.Internal,
        "Attempted to execute task %d times, but it never completed. This task may cause worker %s to crash.",
        t.retryCount+1,
        newWorkerKey(workerID)).Proto(),
}, false)
```

The task is failed with an error message suggesting the task may be crashing the worker.

### Code Flow: Worker Prefers Being Idle (Graceful Termination)

Workers can signal they want to stop receiving new tasks (e.g., for graceful shutdown) by setting `PreferBeingIdle: true` in their `Synchronize()` request.

**1. Worker sends `Synchronize()` with `PreferBeingIdle: true`**

The worker might be preparing to shut down or experiencing issues it needs to resolve.

**2. Scheduler immediately returns idle response**

At [line 2944](in_memory_build_queue.go#L2944), this is checked first in `getNextTask()`:

```go
if preferBeingIdle {
    // The worker wants to terminate or is experiencing some
    // issues. Explicitly instruct the worker to go idle, so
    // that it knows it can hold off synchronizing.
    return bq.getIdleSynchronizeResponse(), nil
}
```

**3. Worker receives idle response**

The worker receives `DesiredState: Idle` immediately — no blocking, no task assignment. The worker can then stop syncing and shut down cleanly.

**Note**: If the worker has a `currentTask`, it must still complete that task. `PreferBeingIdle` only affects *new* task assignment after the current task completes.

### Code Flow: Wrong Action Digest (Worker Reports Different Action)

If a worker reports executing or completing a different action than what the scheduler assigned, the scheduler redirects it to get the correct task.

**1. Worker sends heartbeat with wrong action digest**

The worker calls `Synchronize(Executing)` but the `ActionDigest` doesn't match what the scheduler assigned.

**2. Scheduler detects mismatch in `updateTask()`**

At [line 3087](in_memory_build_queue.go#L3087):

```go
func (w *worker) updateTask(...) (*remoteworker.SynchronizeResponse, error) {
    if !w.isRunningCorrectTask(actionDigest) {
        return w.getCurrentOrNextTask(nil, bq, scq, workerID, preferBeingIdle)
    }
    // ...
}
```

The `nil` context means "don't block" — the worker should get its correct task immediately or go idle.

**3. `isRunningCorrectTask()` comparison**

At [line 3075](in_memory_build_queue.go#L3075):

```go
func (w *worker) isRunningCorrectTask(actionDigest *remoteexecution.Digest) bool {
    t := w.currentTask
    if t == nil {
        return false
    }
    desiredDigest := t.desiredState.ActionDigest
    return proto.Equal(actionDigest, desiredDigest)
}
```

**4. Same handling for `completeTask()`**

At [line 3102](in_memory_build_queue.go#L3102), completion also checks:

```go
func (w *worker) completeTask(...) (*remoteworker.SynchronizeResponse, error) {
    if !w.isRunningCorrectTask(actionDigest) {
        return w.getCurrentOrNextTask(ctx, bq, scq, workerID, preferBeingIdle)
    }
    // ... normal completion ...
}
```

**5. Worker receives correct task or idle**

`getCurrentOrNextTask()` either:
- Re-sends the correct task (if `w.currentTask != nil`)
- Assigns a new task from the queue (if worker has no task)
- Returns idle response (if no work available)

This handles edge cases like workers with corrupted state or protocol bugs.

## Execute Flow

When a client calls `Execute()`, the scheduler creates a task and either assigns it directly to an idle worker or queues it.

### Code Flow: Execute with Idle Worker Available

**1. Client calls `Execute()` with an action digest**

The request enters `Execute()` at [in_memory_build_queue.go:413](in_memory_build_queue.go#L413). The client provides an `ActionDigest` pointing to an `Action` message in the CAS.

**2. Scheduler fetches the Action from CAS**

At [line 440](in_memory_build_queue.go#L440), the scheduler fetches the `Action` message:

```go
actionMessage, err := bq.contentAddressableStorage.Get(ctx, actionDigest).ToProto(
    &remoteexecution.Action{}, bq.maximumMessageSizeBytes)
```

The scheduler needs the `Action` to read `DoNotCache` (for deduplication decisions) and `Platform` (for routing to the correct worker pool). It also passes the `Action` to the worker so the worker doesn't need to fetch it again.

**3. Action is routed and task is created**

After routing via `actionRouter.RouteAction()` at [line 470](in_memory_build_queue.go#L470), a task is created at [line 538](in_memory_build_queue.go#L538):

```go
t := &task{
    operations:   map[*invocation]*operation{},
    actionDigest: actionDigest,
    desiredState: remoteworker.DesiredState_Executing{...},
    ...
}
```

**4. Task is scheduled**

At [line 561](in_memory_build_queue.go#L561), `t.schedule(bq)` is called.

**5. `schedule()` finds an idle worker**

The `schedule()` function at [line 2405](in_memory_build_queue.go#L2405) looks for idle workers. It searches the invocation tree for workers in `idleSynchronizingWorkers`:

```go
if len(i.idleSynchronizingWorkers) > 0 || i.idleSynchronizingWorkersChildren.Len() > 0 {
    // Found idle workers - assign directly
    ...
    i.idleSynchronizingWorkers[0].worker.assignUnqueuedTaskAndWakeUp(bq, t, 0)
    return
}
```

**6. Worker is assigned task and woken up**

`assignUnqueuedTaskAndWakeUp()` at [line 2916](in_memory_build_queue.go#L2916) does two things:

```go
func (w *worker) assignUnqueuedTaskAndWakeUp(bq *InMemoryBuildQueue, t *task, stickinessRetained int) {
    w.wakeUp(t.getCurrentSizeClassQueue())  // Close the wakeup channel
    w.assignUnqueuedTask(bq, t, ...)        // Assign task to worker
}
```

`wakeUp()` at [line 2773](in_memory_build_queue.go#L2773) closes the worker's wakeup channel:

```go
func (w *worker) wakeUp(scq *sizeClassQueue) {
    close(w.wakeup)
    w.dequeue(scq)
}
```

`assignUnqueuedTask()` at [line 2778](in_memory_build_queue.go#L2778) links the task and worker:

```go
w.currentTask = t
t.currentWorker = w
t.retryCount = 0
```

This all happens on the `Execute()` goroutine while holding the lock. The worker's goroutine is blocked in `select{}` and will wake up when it observes the closed channel.

### Code Flow: Execute with No Idle Workers

If all workers are busy, the task is queued and waits for a worker to become available.

**1-4. Same as "Execute with Idle Worker Available"**

Steps 1-4 are identical: client calls `Execute()`, scheduler fetches action, routes it, creates task, and calls `schedule()`.

**5. `schedule()` finds no idle workers, queues the operation**

The `schedule()` function searches the invocation tree but finds no idle workers. When it reaches the root invocation with no workers, it queues the operation at [line 2446](in_memory_build_queue.go#L2446):

```go
if i.parent == nil {
    // Even the root invocation has no idle workers available
    // that are synchronizing against the scheduler.
    //
    // Queue the operation, so that workers can pick it up
    // when they become available.
    t.registerQueuedStageStarted(bq, &scq.tasksScheduledQueue)
    for _, o := range t.operations {
        o.enqueue()
    }
    return
}
```

**6. Operation is added to invocation's queue**

`enqueue()` at [line 2248](in_memory_build_queue.go#L2248) adds the operation to the invocation's priority heap:

```go
func (o *operation) enqueue() {
    i := o.invocation
    i.maybeActivate()
    heap.Push(&i.queuedOperations, o)
    // Also update parent invocations so they know they have queued children
    for i.parent != nil {
        i.updateFirstOperationPriority()
        i.parent.maybeActivate()
        heapPushOrFix(&i.parent.queuedChildren, i.queuedChildrenIndex, i)
        i = i.parent
    }
}
```

The task is now queued. The `Execute()` call blocks in `waitExecution()` until completion.

**7. Worker picks up the task**

When a worker becomes available and calls `Synchronize(Idle)`, it will find this queued task. See "Idle Worker, Jobs Queued" flow above.

### Code Flow: Execute with In-Flight Deduplication

If two clients request the same action simultaneously, the scheduler deduplicates them so the action only executes once.

**1. First client calls `Execute()`**

The first request proceeds as normal (see flow above). When the task is created at [line 555](in_memory_build_queue.go#L555), it's added to the deduplication map:

```go
if !action.DoNotCache {
    bq.inFlightDeduplicationMap[actionDigest] = t
}
```

The task is now executing on a worker.

**2. Second client calls `Execute()` with the same action digest**

After fetching the action and routing, the scheduler acquires the lock and checks the deduplication map at [line 478](in_memory_build_queue.go#L478):

```go
if t, ok := bq.inFlightDeduplicationMap[actionDigest]; ok {
    // A task for the same action digest already exists
    // against which we may deduplicate. No need to create a task.
```

**3. Scheduler checks if same invocation**

At [line 485](in_memory_build_queue.go#L485), the scheduler checks if the existing task already has an operation for this invocation:

```go
i := scq.getOrCreateInvocation(bq, invocationKeys)
if o, ok := t.operations[i]; ok {
    // Task is already associated with the current invocation.
    // Simply wait on the operation that already exists.
    return o.waitExecution(bq, out)
}
```

If yes (same client requesting twice), it just waits on the existing operation.

**4. Different invocation: create new operation for existing task**

If it's a different invocation (different client), a new operation is created for the existing task at [line 494](in_memory_build_queue.go#L494):

```go
o := t.newOperation(bq, in.ExecutionPolicy.GetPriority(), i, false)
switch t.getStage() {
case remoteexecution.ExecutionStage_QUEUED:
    o.enqueue()
case remoteexecution.ExecutionStage_EXECUTING:
    i.incrementExecutingWorkersCount(bq, t.currentWorker)
}
return o.waitExecution(bq, out)
```

No new worker is assigned — the task continues executing on the same worker. The operation is just a bookkeeping object that:
- Gives this client a handle to wait on
- Tracks that this invocation has an "executing worker" for fair scheduling purposes

Now both clients wait on the same task through different operations.

**5. Task completes, both clients notified**

When the worker completes the task, `task.complete()` closes `stageChangeWakeup`, which wakes up all clients waiting in `waitExecution()`. Both receive the same result.

### Code Flow: No Workers for Platform

When a client requests an action for a platform with no registered workers.

**1. Client calls `Execute()` with action**

The action specifies a platform (e.g., `os:linux,arch:arm64`) that has no workers.

**2. Platform queue lookup fails**

At [line 513](in_memory_build_queue.go#L513), the trie lookup returns no match:

```go
platformQueueIndex := bq.platformQueuesTrie.GetLongestPrefix(platformKey)
if platformQueueIndex < 0 {
    // No workers exist for this platform
}
```

**3. Error code depends on scheduler age**

At [line 516](in_memory_build_queue.go#L516), the scheduler checks how long it's been running:

```go
code := codes.FailedPrecondition
if bq.now.Before(bq.platformQueueAbsenceHardFailureTime) {
    // Scheduler just started - workers might still be connecting
    code = codes.Unavailable
}
```

- **FailedPrecondition**: Scheduler has been running long enough that workers should have connected. The platform genuinely doesn't exist.
- **Unavailable**: Scheduler just started. Workers might still be reconnecting after a scheduler restart. Client should retry.

**4. Client receives error**

```go
return status.Errorf(code, "No workers exist for instance name prefix %#v platform %s", ...)
```

The `platformQueueAbsenceHardFailureTime` grace period prevents cascading build failures during scheduler restarts.

### Code Flow: DoNotCache Actions (No Deduplication)

Actions with `DoNotCache: true` are not deduplicated, even if an identical action is already running.

**1. Client calls `Execute()` with `DoNotCache: true` action**

The action's `DoNotCache` field is true (e.g., for non-hermetic builds or actions with side effects).

**2. Deduplication check is skipped**

At [line 478](in_memory_build_queue.go#L478), even if a matching task exists in `inFlightDeduplicationMap`, the scheduler still creates a new task because at [line 555](in_memory_build_queue.go#L555):

```go
if !action.DoNotCache {
    bq.inFlightDeduplicationMap[actionDigest] = t
}
```

The new task is **not** added to the deduplication map, so:
- It won't be found by future requests
- Future requests also won't find it

**3. New task executes independently**

A separate worker executes this task, even if an identical action is already running elsewhere. This is correct for non-hermetic actions where each execution might produce different results.

### Code Flow: Client Disconnects (Operation Abandonment)

When a client stops waiting for a task (disconnect, timeout, cancellation).

**1. Client's `waitExecution()` context is canceled**

The client closes the connection or cancels the request. At [line 2215](in_memory_build_queue.go#L2215):

```go
select {
case <-ctx.Done():
    timer.Stop()
    bq.enter(bq.clock.Now())
    return util.StatusFromContext(ctx)
```

**2. Waiter count decreases**

The `defer` at [line 2169](in_memory_build_queue.go#L2169) runs:

```go
defer func() {
    o.waiters--
    o.maybeStartCleanup(bq)
}()
```

**3. Cleanup timer starts if no waiters remain**

At [line 2328](in_memory_build_queue.go#L2328), `maybeStartCleanup()` schedules removal:

```go
func (o *operation) maybeStartCleanup(bq *InMemoryBuildQueue) {
    if o.waiters == 0 && !o.mayExistWithoutWaiters {
        bq.cleanupQueue.add(&o.cleanupKey, bq.now.Add(bq.configuration.OperationWithNoWaitersTimeout), func() {
            o.remove(bq)
        })
    }
}
```

The operation isn't removed immediately — `OperationWithNoWaitersTimeout` gives the client time to reconnect via `WaitExecution()`.

**4a. Client reconnects before timeout**

If the client calls `WaitExecution()` with the operation name before the timeout, the cleanup is canceled and waiting resumes.

**4b. Timeout fires, operation removed**

If no client reconnects, `operation.remove()` is called at [line 2260](in_memory_build_queue.go#L2260).

**5. Task canceled if no operations remain**

At [line 2264](in_memory_build_queue.go#L2264), if this was the last operation:

```go
if len(t.operations) == 1 {
    // Forcefully terminate the associated task
    t.complete(bq, &remoteexecution.ExecuteResponse{
        Status: status.New(codes.Canceled, "Task no longer has any waiting clients").Proto(),
    }, false)
}
```

The task is canceled. If a worker was executing it, the worker's next heartbeat will get a new task (the canceled task's worker link is severed by `complete()`).

**Note**: If multiple clients were waiting (via deduplication), only the operation for the disconnected client is cleaned up. The task continues for other waiting clients.

## Task Completion Flow

When a worker finishes executing a task, it reports results back to the scheduler, which notifies waiting clients and performs cleanup.

### Code Flow: Worker Completes Task Successfully

**1. Worker sends `Synchronize()` with `CurrentState_Executing` and `Completed`**

After finishing execution, the worker calls `Synchronize()` with:
- `CurrentState_Executing` containing the action digest
- `ExecutionState: Completed` containing the `ExecuteResponse`

At [line 706](in_memory_build_queue.go#L706), the scheduler routes to `completeTask()`:

```go
case *remoteworker.CurrentState_Executing_Completed:
    return w.completeTask(ctx, bq, scq, request.WorkerId, executing.ActionDigest, executionState.Completed, request.PreferBeingIdle)
```

**2. `completeTask()` validates and delegates**

At [line 3103](in_memory_build_queue.go#L3103), the worker validates it's completing the correct task, then calls `task.complete()`:

```go
func (w *worker) completeTask(...) (*remoteworker.SynchronizeResponse, error) {
    if !w.isRunningCorrectTask(actionDigest) {
        return w.getCurrentOrNextTask(ctx, bq, scq, workerID, preferBeingIdle)
    }
    w.currentTask.complete(bq, executeResponse, true)
    return w.getNextTask(ctx, bq, scq, workerID, preferBeingIdle)
}
```

After completion, the worker immediately requests its next task via `getNextTask()`.

**3. `task.complete()` updates worker stickiness**

At [line 2471](in_memory_build_queue.go#L2471), the completion logic begins. First, it computes the lowest common ancestor of all invocations associated with the task (for deduplication cases) and sets `worker.lastInvocation` to preserve locality:

```go
// Find lowest common ancestor of all invocations
var iLowest *invocation
for i := range t.operations {
    // ... LCA algorithm ...
}
w.setLastInvocation(iLowest)
```

This means the worker will preferentially pick up future tasks from the same invocation tree.

**4. Worker-task link is severed**

At [line 2523](in_memory_build_queue.go#L2523):

```go
for i := range t.operations {
    i.decrementExecutingWorkersCount(bq, t.currentWorker)
}
t.currentWorker.currentTask = nil
t.currentWorker = nil
```

**5. Size class learner receives feedback**

At [line 2535](in_memory_build_queue.go#L2535), the scheduler checks if the task succeeded (status OK and exit code 0):

```go
if code, actionResult := status.FromProto(executeResponse.Status).Code(), executeResponse.Result; code == codes.OK && actionResult.GetExitCode() == 0 {
    // Task succeeded
    _, _, _, backgroundInitialSizeClassLearner := t.initialSizeClassLearner.Succeeded(
        executionMetadata.GetVirtualExecutionDuration().AsDuration(),
        pq.sizeClasses)
```

The learner records the execution duration for future size class decisions.

**6. Task is finalized and clients notified**

At [line 2609](in_memory_build_queue.go#L2609), the task is finalized:

```go
delete(bq.inFlightDeduplicationMap, t.actionDigest)  // Remove from dedup map
t.executeResponse = executeResponse                   // Store result
t.desiredState.Action = nil                          // Free memory
close(t.stageChangeWakeup)                           // Wake up all waiting clients
```

Closing `stageChangeWakeup` unblocks all clients in `waitExecution()`.

**7. Clients receive results**

Each client blocked in `waitExecution()` at [line 2219](in_memory_build_queue.go#L2219) wakes up:

```go
select {
case <-stageChangeWakeup:  // Channel closed - task completed
    bq.enter(bq.clock.Now())
    // Loop continues, builds response with executeResponse
}
```

On the next loop iteration, `t.executeResponse != nil`, so the response is sent with `Done: true`:

```go
if t.executeResponse != nil {
    operation.Done = true
    operation.Result = &longrunningpb.Operation_Response{Response: response}
}
// ...
if err := out.Send(operation); operation.Done || err != nil {
    return err  // Exit loop, client receives result
}
```

### Code Flow: Task Fails, Retry on Larger Size Class

When a task fails on a smaller size class, the scheduler can automatically retry on a larger worker.

**1-4. Same as successful completion**

Worker reports completion, stickiness is updated, worker-task link is severed.

**5. Size class learner requests retry**

At [line 2580](in_memory_build_queue.go#L2580), the failure path is taken:

```go
} else if completedByWorker {
    expectedDuration, timeout, t.initialSizeClassLearner = t.initialSizeClassLearner.Failed(code == codes.DeadlineExceeded)
}
```

If `Learner.Failed()` returns a non-nil learner, the scheduler should retry.

**6. Task is moved to largest size class**

At [line 2591](in_memory_build_queue.go#L2591):

```go
if t.initialSizeClassLearner != nil {
    // Re-execution against the largest size class is requested
    t.expectedDuration = expectedDuration
    t.desiredState.Action.Timeout = durationpb.New(timeout)

    largestSCQ := pq.sizeClassQueues[len(pq.sizeClassQueues)-1]
    // Transplant operations to largest size class queue
    for oldI, o := range operations {
        i := largestSCQ.getOrCreateInvocation(bq, oldI.invocationKeys)
        t.operations[i] = o
        o.invocation = i
    }
    t.schedule(bq)              // Re-enter scheduling
    t.reportNonFinalStageChange()  // Notify clients of stage change
}
```

The task is rescheduled on the largest size class. Clients are **not** notified of completion — they continue waiting while the retry executes.

**7. Retry executes on larger worker**

The task goes through the normal scheduling flow (see Execute Flow) on the largest size class. When it completes (success or failure), `task.complete()` is called again. This time, `initialSizeClassLearner` will be nil after `Failed()` returns, so the task finalizes.

### Code Flow: Background Learning Task

When a task succeeds on a large worker but the learner wants to test if it could run on a smaller one, a background task is created.

**1-5. Same as successful completion**

**6. Learner requests background execution**

At [line 2546](in_memory_build_queue.go#L2546), `Succeeded()` may return a new learner:

```go
if backgroundInitialSizeClassLearner != nil {
    if pq.maximumQueuedBackgroundLearningOperations == 0 {
        backgroundInitialSizeClassLearner.Abandoned()
    } else {
        // Create background task
        backgroundTask := &task{
            actionDigest:            t.actionDigest,
            initialSizeClassLearner: backgroundInitialSizeClassLearner,
            // ...
        }
        backgroundAction.DoNotCache = true  // Don't overwrite cached result
        backgroundTask.schedule(bq)
    }
}
```

**7. Original task completes normally**

The original task still completes and clients receive results. The background task runs independently for learning purposes only — its result is not returned to any client.

## Resilience

The scheduler handles various failure modes. Each scenario below describes the preconditions and the scheduler's response.

### Clean Disconnect

| | |
|---|---|
| **Precondition** | Worker connection closes cleanly (TCP FIN/RST or client cancellation) |
| **Detection** | gRPC cancels the request context, `ctx.Done()` becomes readable |
| **Response** | Worker is immediately dequeued from `idleSynchronizingWorkers` |

### Silent Connection Failure

| | |
|---|---|
| **Precondition** | Worker becomes unreachable without closing connection (network partition, killed VM, frozen process) |
| **Detection** | gRPC cannot detect this immediately since no TCP FIN was sent. Without intervention, the scheduler's blocking `select{}` would wait forever. |
| **Response** | Two timeouts provide protection: |

1. `GetIdleWorkerSynchronizationInterval`: Maximum time the server blocks before returning an idle response. Forces periodic reconnection, detecting half-open connections.
2. `WorkerWithNoSynchronizationsTimeout`: If a worker doesn't call `Synchronize()` within this window, cleanup removes the worker (see Stale Worker below).

### Task Delivery Failure

| | |
|---|---|
| **Precondition** | Scheduler assigned a task to a worker, but the worker reports idle on next sync (network flakiness lost the response, worker crashed and restarted, or worker bug) |
| **Detection** | Worker calls `Synchronize(Idle)` but `worker.currentTask != nil` |
| **Response** | Re-send the task, up to `WorkerTaskRetryCount` times. If retries exhausted, fail the task with an error indicating it may be crashing the worker. |

### Stale Worker (with Executing Task)

| | |
|---|---|
| **Precondition** | Worker stops calling `Synchronize()` while executing a task (crash, network partition, VM killed, etc.) |
| **Detection** | Each `Synchronize()` schedules a cleanup callback at `now + WorkerWithNoSynchronizationsTimeout`. If the worker syncs again, the callback is rescheduled. If not, the timeout fires. |
| **Response** | Task is failed with `Unavailable` error, worker is removed. See [Worker Stops Heartbeating](#code-flow-worker-stops-heartbeating-stale-worker) for full code flow. |
