# Worker Node Health Check Design

## Goal

After every job, run a node health check. If the check fails, stop accepting
work so the node can be terminated by the infrastructure layer.

## Constraints

- Workers run with concurrency=1 (single execution slot per node)
- Custom hardware that tests use exclusively (cannot be shared across jobs)
- Custom hardware requires per-node health validation between jobs
- Job results must always be reported to the scheduler before the node is removed
- Single worker per node, running as a dedicated non-root user via systemd

## Approach

Combine two existing bb_runner mechanisms:

1. **`run_command_cleaner`** — runs a health check script after every job
2. **`readiness_checking_pathnames`** — gates whether the worker accepts new work

The cleaner script performs the health check and, on failure, removes a sentinel
file. The readiness check sees the missing file on the next iteration and stops
the worker from picking up more work. The infrastructure layer (Kubernetes
liveness probe, systemd watchdog, etc.) then terminates the node.

## Execution Flow

With concurrency=1, the `IdleInvoker` use count goes 0→1 (Acquire) before every
job and 1→0 (Release) after, so the cleaner chain fires reliably between each
action. The full between-job sequence is:

```
Job completes
    │
    ▼
idleInvoker.Release() fires chained cleaners (in order):
    1. ProcessTableCleaner — kills leftover daemonized processes
    2. DirectoryCleaner    — wipes temporary directories
    3. CommandRunningCleaner — runs health check script
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

Note: with concurrency > 1, the cleaners only fire on the idle<->busy boundary
(when going from all slots free to one busy, and vice versa), not between every
job. Concurrency=1 is required for per-job health checks.

## Why Not Terminate in the Cleaner Directly?

`run_command_cleaner` executes inside `cleanRunner.Run()`, **before** the job
result is reported to the scheduler. If the cleaner script kills the node, the
scheduler never learns the action completed. It would time out and reschedule
the action on another worker.

By deferring the actual "stop accepting work" decision to the readiness check
(which runs after the result is reported), the job result is always safe.

## Process Cleanup

Enable `clean_process_table: true` to kill daemonized processes left behind by
build actions between jobs. The cleaner enumerates `/proc`, then filters to only
kill processes that:

1. Have the **same user ID** as bb_runner (or the `run_commands_as` user)
2. Were **created after** bb_runner started

This prevents it from killing unrelated system processes. Since we run a single
worker per node under a dedicated user, this is safe without `run_commands_as`.

## Configuration

### bb_runner

```jsonc
{
  // Kill daemonized processes left behind by build actions.
  "clean_process_table": true,

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

### systemd Unit

Run bb_runner as a dedicated non-root user. The process table cleaner
automatically uses the UID of the bb_runner process (via `os.Getuid()`), so
no `run_commands_as` is needed.

```ini
[Unit]
Description=Buildbarn Runner
After=network.target

[Service]
User=bbworker
Group=bbworker
ExecStartPre=/bin/mkdir -p /var/run/bb-worker
ExecStartPre=/bin/touch /var/run/bb-worker/healthy
ExecStart=/usr/local/bin/bb_runner /etc/bb_runner/config.jsonnet
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Create the system user:

```bash
useradd --system --no-create-home --shell /usr/sbin/nologin bbworker
```

Ensure `bbworker` owns the build directory and sentinel file path.
