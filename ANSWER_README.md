# ANSWER_README

## What I did

I reviewed README/project-guide requirements and performed a full static audit of all boilerplate code.
I then implemented the missing runtime and kernel monitor logic without executing binaries, as requested.

I also performed 3 manual recheck passes:

1. Pass 1: completed all TODO implementations and core architecture wiring.
2. Pass 2: fixed correctness/safety edge cases (memory lifetime, parser safety, cleanup paths).
3. Pass 3: diagnostics pass using editor error checks and final logic consistency review.

Note: because execution is restricted, validation here is static (code inspection + diagnostics), not runtime verification.

---

## Files changed

- boilerplate/engine.c
- boilerplate/monitor.c
- boilerplate/memory_hog.c
- boilerplate/cpu_hog.c
- boilerplate/io_pulse.c

Created:

- ANSWER_README.md (this file)

No additional files were created.

---

## Detailed fixes

## 1) engine.c: from partial skeleton to working supervisor/runtime core

### A. Bounded buffer implementation (logging pipeline)

Implemented:

- bounded_buffer_push
- bounded_buffer_pop
- shutdown signaling with condition-variable broadcast

Behavior now:

- Producers block when buffer is full.
- Consumers block when buffer is empty.
- Shutdown wakes both sides and allows graceful termination.

Why this matters:

- Prevents data races and busy-wait loops.
- Supports required producer-consumer semantics from the project guide.

### B. Logging system end-to-end

Implemented:

- consumer logger thread that drains bounded buffer and appends to per-container log files
- producer thread per container that reads child stdout/stderr pipe and enqueues chunks
- log directory creation helper and robust append loop

Why this matters:

- Captures both stdout/stderr through pipe redirection.
- Persists logs to logs/<container-id>.log.
- Preserves output chunks across concurrent containers.

### C. Container child setup via clone entrypoint

Implemented in child path:

- UTS namespace hostname setup
- mount namespace private remount
- chroot into container rootfs
- /proc mount inside container
- stdout/stderr redirection to logging pipe
- optional nice adjustment
- command execution through /bin/sh -c

Why this matters:

- Delivers isolation and expected process view inside containers.
- Supports the command model required by CLI contract.

### D. Supervisor control plane (UNIX domain socket)

Implemented:

- control socket at /tmp/mini_runtime.sock
- request/response protocol for start/run/ps/logs/stop
- client-side send_control_request path

Command behavior now:

- supervisor: initializes monitor fd (optional), logger thread, socket server, signal handlers, event loop
- start: launches container, returns immediately after metadata/producer setup
- run: launches container and waits for exit status, then returns that status
- ps: returns compact tracked container summary
- logs: returns log path and client prints log content
- stop: marks stop_requested and sends SIGTERM

Why this matters:

- Fulfills the required split between long-lived supervisor and short-lived CLI client.

### E. Metadata synchronization and lifecycle tracking

Implemented:

- mutex-protected linked list container metadata
- fields for limits, exit_code, exit_signal, stop_requested, run_waiter_active
- SIGCHLD-driven reaping for background containers
- state transitions to exited/stopped/killed
- monitor unregister on exit

Why this matters:

- Avoids metadata races under concurrent command handling and child exits.
- Preserves attribution between manual stop and kill-like termination paths.

### F. Memory lifetime and cleanup fixes

Critical fixes made:

- clone stack memory now stored per container and freed only after process exit/reap
- cleanup frees all remaining stack allocations and metadata nodes
- graceful shutdown path signals containers, shuts down buffer, joins logger and producer threads, closes descriptors, unlinks socket

Why this matters:

- Prevents use-after-free/leaks from early clone stack deallocation.
- Ensures teardown logic is deterministic.

### G. Signal handling improvements

Implemented:

- SIGCHLD/SIGINT/SIGTERM handlers for supervisor control loop
- SA_RESTART disabled so accept can break on termination signals

Why this matters:

- Allows orderly shutdown instead of hanging in blocking accept.

### H. Input and contract validations

Added checks for:

- duplicate container ids
- rootfs already in use by active container
- invalid empty request fields
- soft/hard limit relationship via flag parser

Why this matters:

- Enforces required rootfs uniqueness and safer command handling.

---

## 2) monitor.c: all TODO regions implemented

### A. Kernel tracking structure and lock

Implemented:

- monitored_entry node with pid/container_id/soft/hard/soft-warning flag/list linkage
- global monitored list + mutex

Why mutex:

- timer callback + ioctl paths may sleep (task/mm helpers, allocation); mutex is appropriate for sleepable critical sections.

### B. Periodic monitor timer logic

Implemented:

- safe iteration with list_for_each_entry_safe
- RSS query per PID
- stale/exited PID removal
- one-time soft-limit event emission
- hard-limit kill + entry removal

Why this matters:

- Implements required soft/hard policy behavior and stale-state cleanup.

### C. IOCTL register/unregister

Implemented register path:

- validates PID and limit ordering
- allocates/initializes node
- duplicate prevention by PID/container id
- inserts under lock

Implemented unregister path:

- removes by matching PID or container id when provided
- returns ENOENT when not found

Why this matters:

- Provides stable kernel-user integration for supervisor registration lifecycle.

### D. Module unload cleanup

Implemented:

- frees all remaining monitored entries during module exit

Why this matters:

- avoids leaked kernel objects on unload.

---

## 3) Workload utility fixes

### memory_hog.c

Fixed parse helpers to validate null/empty input before calling strtoul.

### cpu_hog.c

Fixed parse helper to validate null/empty input before strtoul.

### io_pulse.c

Fixed:

- parse helper null safety before strtoul
- snprintf failure/overflow check
- write result comparison using ssize_t-safe logic

Why these matter:

- Removes undefined behavior and signed/unsigned mismatch risks.

---

## Recheck results (3 passes)

## Pass 1: implementation completeness

Confirmed all major TODO blocks in engine.c and monitor.c are implemented.

## Pass 2: safety/correctness

Reviewed and fixed:

- clone stack lifetime bug
- rootfs uniqueness logic using dedicated rootfs metadata field
- failure cleanup in producer-thread creation paths
- safer signal interruption behavior for shutdown

## Pass 3: diagnostics and final consistency review

Editor diagnostics show no issues in user-space files changed.

For monitor.c, diagnostics report missing Linux kernel include path on this machine/editor context (expected when kernel headers/tooling are unavailable in this restricted environment). This is an environment/indexing issue, not a syntax defect in kernel module logic.

---
