# Multi-Node Assignment Test Plan

## Status: IMPLEMENTED ✓

The test has been implemented in [in_memory_build_queue_test.go](pkg/scheduler/in_memory_build_queue_test.go) at line 744.

## Test: `TestInMemoryBuildQueueExecuteMultinodeAssignment`

**Goal:** Verify that when N workers are idle, a multi-node task group is assigned to all of them.

## The Challenge

Workers call `Synchronize()` which blocks until work is available. When worker N syncs, it triggers assignment to workers 1..N-1 (who are blocking) and itself.

## Test Flow (multinode.count=2)

### Phase 1: Setup

1. Create buildQueue with mocks
2. Register 2 workers using Executing state (to establish platform)
   - Worker 0: `{"hostname": "worker", "thread": "0"}`
   - Worker 1: `{"hostname": "worker", "thread": "1"}`

### Phase 2: Submit Multi-Node Action

1. Call `Execute()` with action containing `multinode.count=2`
2. Receive QUEUED update for operation

### Phase 3: Worker 0 Syncs Idle (Blocks)

1. Worker 0 calls `Synchronize()` with Idle state
2. `assignNextQueuedTask()` sees multi-node at head
3. Checks `countIdleSynchronizingWorkers()` = 0
4. Needs 1 synchronizing worker, has 0 → returns false
5. Worker enters blocking loop in `getNextTask()`
6. Gets added to `idleSynchronizingWorkers`
7. **Blocks waiting for wakeup channel**

### Phase 4: Worker 1 Syncs Idle (Triggers Assignment)

1. Worker 1 calls `Synchronize()` with Idle state
2. `assignNextQueuedTask()` sees multi-node at head
3. Checks `countIdleSynchronizingWorkers()` = 1
4. Needs 1 synchronizing worker, has 1 → **success!**
5. `assignMultinodeTaskGroup()` is called:
   - Task 0 → assigned to Worker 1 (current worker) via `assignUnqueuedTask()`
   - Task 1 → assigned to Worker 0 (from idleSynchronizingWorkers) via `assignUnqueuedTaskAndWakeUp()`
6. `assignUnqueuedTaskAndWakeUp()` closes Worker 0's wakeup channel
7. Worker 0 unblocks and returns
8. Worker 1 returns immediately with Executing response

### Phase 5: Verify Results

- Worker 1: Returns Executing response (synchronously)
- Worker 0: Returns Executing response (after being woken up)
- Both responses have the same action digest

## Mock Expectations

### Initial Setup
```go
clock.EXPECT().Now().Return(time.Unix(0, 0))  // buildQueue creation
```

### Register Workers (x2)
```go
clock.EXPECT().Now().Return(time.Unix(1000, 0))  // worker 0
clock.EXPECT().Now().Return(time.Unix(1001, 0))  // worker 1
```

### Submit Action
```go
contentAddressableStorage.EXPECT().Get(...)
actionRouter.EXPECT().RouteAction(...)
initialSizeClassSelector.EXPECT().Select(...)
clock.EXPECT().Now().Return(time.Unix(1010, 0))
uuidGenerator.EXPECT().Call().Return(...)  // x2 for 2 tasks
clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)  // waitExecution timer
```

### Worker 0 Syncs Idle (Blocks)
```go
clock.EXPECT().Now().Return(time.Unix(1020, 0))
idleTimer0 := mock.NewMockTimer(ctrl)
idleTimerChan0 := make(chan time.Time)  // unbuffered, never fires
clock.EXPECT().NewTimer(time.Minute).Return(idleTimer0, idleTimerChan0)
idleTimer0.EXPECT().Stop().Return(true)  // Called in defer when worker wakes up
```

### Worker 1 Syncs Idle (Triggers Assignment)
```go
clock.EXPECT().Now().Return(time.Unix(1021, 0))
// No timer created - worker gets work immediately
```

## Key Insight

Worker 0's timer `Stop()` is called when the deferred cleanup in `getNextTask()` runs after the function returns (due to being woken up by Worker 1's assignment).

The wakeup mechanism:
1. Worker 0 creates `wakeup := make(chan struct{})` and blocks on it
2. `assignUnqueuedTaskAndWakeUp()` calls `w.wakeUp()` which does `close(w.wakeup)`
3. Worker 0's select unblocks, it checks `w.currentTask != nil` → true
4. Returns `getExecutingSynchronizeResponse()`
5. Defer runs `timeoutTimer.Stop()`

## Test Code Structure

```go
func TestInMemoryBuildQueueExecuteMultinodeAssignment(t *testing.T) {
    ctrl, ctx := gomock.WithContext(context.Background(), t)

    // Setup mocks
    contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
    clock := mock.NewMockClock(ctrl)
    clock.EXPECT().Now().Return(time.Unix(0, 0))
    uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
    actionRouter := mock.NewMockActionRouter(ctrl)

    buildQueue := scheduler.NewInMemoryBuildQueue(...)
    executionClient := getExecutionClient(t, buildQueue)

    // Register 2 workers with Executing state
    for i := 0; i < 2; i++ {
        clock.EXPECT().Now().Return(time.Unix(1000+int64(i), 0))
        _, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
            WorkerId: map[string]string{"hostname": "worker", "thread": fmt.Sprintf("%d", i)},
            InstanceNamePrefix: "main",
            Platform: platformForTesting,
            CurrentState: &remoteworker.CurrentState{
                WorkerState: &remoteworker.CurrentState_Executing_{...},
            },
        })
        require.NoError(t, err)
    }

    // Submit multi-node action
    // ... setup action, CAS mock, actionRouter mock, etc ...
    clock.EXPECT().Now().Return(time.Unix(1010, 0))
    uuidGenerator.EXPECT().Call().Return(uuid.Parse("..."))  // task 0
    uuidGenerator.EXPECT().Call().Return(uuid.Parse("..."))  // task 1
    timer := mock.NewMockTimer(ctrl)
    clock.EXPECT().NewTimer(time.Minute).Return(timer, nil)

    stream, err := executionClient.Execute(ctx, executeRequest)
    require.NoError(t, err)

    update, err := stream.Recv()
    require.NoError(t, err)
    // verify QUEUED

    // Worker 0 syncs idle - will block
    clock.EXPECT().Now().Return(time.Unix(1020, 0))
    idleTimer0 := mock.NewMockTimer(ctrl)
    idleTimerChan0 := make(chan time.Time)
    clock.EXPECT().NewTimer(time.Minute).Return(idleTimer0, idleTimerChan0)
    idleTimer0.EXPECT().Stop().Return(true)

    worker0Done := make(chan *remoteworker.SynchronizeResponse, 1)
    go func() {
        response, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
            WorkerId: map[string]string{"hostname": "worker", "thread": "0"},
            InstanceNamePrefix: "main",
            Platform: platformForTesting,
            CurrentState: &remoteworker.CurrentState{
                WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}},
            },
        })
        require.NoError(t, err)
        worker0Done <- response
    }()

    // Wait for worker 0 to enter idleSynchronizingWorkers
    time.Sleep(50 * time.Millisecond)

    // Worker 1 syncs idle - triggers assignment
    clock.EXPECT().Now().Return(time.Unix(1021, 0))
    response1, err := buildQueue.Synchronize(ctx, &remoteworker.SynchronizeRequest{
        WorkerId: map[string]string{"hostname": "worker", "thread": "1"},
        InstanceNamePrefix: "main",
        Platform: platformForTesting,
        CurrentState: &remoteworker.CurrentState{
            WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}},
        },
    })
    require.NoError(t, err)

    // Worker 1 gets Executing immediately
    require.NotNil(t, response1.GetDesiredState().GetExecuting())
    require.Equal(t, "da39a3ee...", response1.GetDesiredState().GetExecuting().GetActionDigest().GetHash())

    // Worker 0 should be woken up and get Executing
    select {
    case response0 := <-worker0Done:
        require.NotNil(t, response0.GetDesiredState().GetExecuting())
        require.Equal(t, "da39a3ee...", response0.GetDesiredState().GetExecuting().GetActionDigest().GetHash())
    case <-time.After(time.Second):
        t.Fatal("Worker 0 did not receive response in time")
    }
}
```

## Potential Issues

1. **Race condition with time.Sleep:** The 50ms sleep might not be enough for Worker 0 to fully enter the blocking state. Could use a sync mechanism instead.

2. **Mock ordering:** gomock expectations might fire in unexpected order with goroutines.

3. **Cleanup:** If test fails, goroutines might leak. Consider using `t.Cleanup()` or context cancellation.

## Alternative Approach: Use Channels for Synchronization

Instead of `time.Sleep`, we could use gomock's `Do()` to signal when expectations are met:

```go
worker0InQueue := make(chan struct{})
clock.EXPECT().NewTimer(time.Minute).DoAndReturn(func(d time.Duration) (clock.Timer, <-chan time.Time) {
    close(worker0InQueue)  // Signal that worker 0 is about to block
    return idleTimer0, idleTimerChan0
})

// ... start worker 0 goroutine ...

<-worker0InQueue  // Wait for worker 0 to be ready
// Now safe to sync worker 1
```

This is more deterministic than `time.Sleep`.
