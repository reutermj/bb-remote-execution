# Worker Node Health Check Design

## Goal

After every job, run a node health check. If the check fails, stop accepting
work so the node can be terminated by the infrastructure layer.

## Constraints

- Workers run with concurrency=1 (single execution slot per node)
- Custom hardware requires per-node health validation between jobs
- Job results must always be reported to the scheduler before the node is removed

## Approach

Combine two existing bb_runner mechanisms:

1. **`run_command_cleaner`** — runs a health check script after every job
2. **`readiness_checking_pathnames`** — gates whether the worker accepts new work

The cleaner script performs the health check and, on failure, removes a sentinel
file. The readiness check sees the missing file on the next iteration and stops
the worker from picking up more work. The infrastructure layer (Kubernetes
liveness probe, systemd watchdog, etc.) then terminates the node.

## Execution Flow

```
Job completes
    │
    ▼
idleInvoker.Release() fires run_command_cleaner
    │
    ▼
Health check script runs
    ├─ passes → sentinel file left in place, script exits 0
    └─ fails  → removes sentinel file, script exits 0
    │
    ▼
cleanRunner.Run() returns (no error either way)
    │
    ▼
Build executor uploads outputs, reports result to scheduler
    │
    ▼
Next loop iteration: CheckReadiness() checks readiness_checking_pathnames
    ├─ sentinel file exists → worker accepts next job
    └─ sentinel file missing → readiness check fails → worker enters retry loop
                                                            │
                                                            ▼
                                                 Infrastructure layer detects
                                                 unhealthy node and terminates it
```

## Why Not Terminate in the Cleaner Directly?

`run_command_cleaner` executes inside `cleanRunner.Run()`, **before** the job
result is reported to the scheduler. If the cleaner script kills the node, the
scheduler never learns the action completed. It would time out and reschedule
the action on another worker.

By deferring the actual "stop accepting work" decision to the readiness check
(which runs after the result is reported), the job result is always safe.

## Configuration

### bb_runner

```jsonc
{
  // Health check script. Runs on every idle<->busy transition.
  // Must always exit 0. On health check failure, removes the sentinel file.
  "run_command_cleaner": ["/usr/local/bin/health-check.sh"],

  // Sentinel file that must exist for the worker to accept work.
  "readiness_checking_pathnames": ["/var/run/bb-worker/healthy"]
}
```

### Health Check Script

```bash
#!/usr/bin/env bash
# /usr/local/bin/health-check.sh

SENTINEL="/var/run/bb-worker/healthy"

if ! /usr/local/bin/check-hardware; then
    rm -f "$SENTINEL"
fi

# Always exit 0 so the current job's result is not affected.
exit 0
```

### Sentinel File Initialization

The sentinel file must be created at node startup before bb_runner starts,
e.g. in a systemd ExecStartPre or Kubernetes init container:

```bash
mkdir -p /var/run/bb-worker
touch /var/run/bb-worker/healthy
```
