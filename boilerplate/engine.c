/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int run_waiter_active;
    void *child_stack;
    char rootfs[PATH_MAX];
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct supervisor_ctx supervisor_ctx_t;

typedef struct producer_arg {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    int pipe_fd;
} producer_arg_t;

typedef struct producer_handle {
    pthread_t tid;
    int joined;
    struct producer_handle *next;
} producer_handle_t;

struct supervisor_ctx {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    producer_handle_t *producers;
    char base_rootfs[PATH_MAX];
};

static volatile sig_atomic_t g_supervisor_stop = 0;
static volatile sig_atomic_t g_supervisor_sigchld = 0;

static void supervisor_signal_handler(int sig)
{
    if (sig == SIGCHLD) {
        g_supervisor_sigchld = 1;
        return;
    }

    if (sig == SIGINT || sig == SIGTERM)
        g_supervisor_stop = 1;
}

static int install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGCHLD, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;

    return 0;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int ensure_log_dir(void)
{
    struct stat st;

    if (stat(LOG_DIR, &st) == 0)
        return S_ISDIR(st.st_mode) ? 0 : -1;

    if (mkdir(LOG_DIR, 0755) < 0)
        return -1;

    return 0;
}

static int append_log(const char *path, const char *data, size_t len)
{
    int fd;
    ssize_t written;

    fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0)
        return -1;

    while (len > 0) {
        written = write(fd, data, len);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            close(fd);
            return -1;
        }
        data += written;
        len -= (size_t)written;
    }

    close(fd);
    return 0;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *it = ctx->containers;

    while (it) {
        if (strcmp(it->id, id) == 0)
            return it;
        it = it->next;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *it = ctx->containers;

    while (it) {
        if (it->host_pid == pid)
            return it;
        it = it->next;
    }

    return NULL;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *it = ctx->containers;

    while (it) {
        if ((it->state == CONTAINER_STARTING || it->state == CONTAINER_RUNNING) &&
            strcmp(it->rootfs, rootfs) == 0)
            return 1;
        it = it->next;
    }

    return 0;
}

static int add_container_locked(supervisor_ctx_t *ctx, const control_request_t *req, pid_t child_pid)
{
    container_record_t *rec;

    if (find_container_locked(ctx, req->container_id) != NULL)
        return -1;

    rec = calloc(1, sizeof(*rec));
    if (!rec)
        return -1;

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->host_pid = child_pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code = -1;
    rec->exit_signal = 0;
    rec->stop_requested = 0;
    rec->run_waiter_active = (req->kind == CMD_RUN) ? 1 : 0;
    rec->child_stack = NULL;
    strncpy(rec->rootfs, req->rootfs, sizeof(rec->rootfs) - 1);
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);

    rec->next = ctx->containers;
    ctx->containers = rec;
    return 0;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    for (;;) {
        pid = waitpid(-1, &status, WNOHANG);
        if (pid <= 0)
            break;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = find_container_by_pid_locked(ctx, pid);
        if (rec && !rec->run_waiter_active) {
            if (WIFEXITED(status)) {
                rec->state = CONTAINER_EXITED;
                rec->exit_code = WEXITSTATUS(status);
                rec->exit_signal = 0;
            } else if (WIFSIGNALED(status)) {
                rec->exit_code = 128 + WTERMSIG(status);
                rec->exit_signal = WTERMSIG(status);
                rec->state = (rec->stop_requested || rec->exit_signal == SIGTERM)
                                 ? CONTAINER_STOPPED
                                 : CONTAINER_KILLED;
            }

            if (ctx->monitor_fd >= 0)
                (void)unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);

            free(rec->child_stack);
            rec->child_stack = NULL;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static void stop_all_containers(supervisor_ctx_t *ctx)
{
    container_record_t *it;

    pthread_mutex_lock(&ctx->metadata_lock);
    it = ctx->containers;
    while (it) {
        if (it->state == CONTAINER_RUNNING || it->state == CONTAINER_STARTING) {
            it->stop_requested = 1;
            (void)kill(it->host_pid, SIGTERM);
        }
        it = it->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void cleanup_metadata(supervisor_ctx_t *ctx)
{
    container_record_t *it;

    pthread_mutex_lock(&ctx->metadata_lock);
    it = ctx->containers;
    while (it) {
        container_record_t *next = it->next;
        free(it->child_stack);
        free(it);
        it = next;
    }
    ctx->containers = NULL;
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void *log_producer_thread(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *)arg;
    char chunk[LOG_CHUNK_SIZE];

    for (;;) {
        ssize_t n = read(pa->pipe_fd, chunk, sizeof(chunk));
        if (n == 0)
            break;
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pa->container_id, sizeof(item.container_id) - 1);
        item.length = (size_t)n;
        memcpy(item.data, chunk, (size_t)n);

        if (bounded_buffer_push(&pa->ctx->log_buffer, &item) != 0)
            break;
    }

    close(pa->pipe_fd);
    free(pa);
    return NULL;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    char path[PATH_MAX];

    for (;;) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        (void)append_log(path, item.data, item.length);
    }

    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (sethostname(cfg->id, strnlen(cfg->id, sizeof(cfg->id))) < 0) {
        perror("sethostname");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount private");
        return 1;
    }

    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }

    if (chroot(".") < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    (void)mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount proc");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    if (cfg->nice_value != 0 && setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    strncpy(ctx.base_rootfs, rootfs, sizeof(ctx.base_rootfs) - 1);

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: cannot open /dev/container_monitor, continuing without kernel monitor\n");

    if (ensure_log_dir() != 0) {
        perror("ensure_log_dir");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    {
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("bind");
            close(ctx.server_fd);
            unlink(CONTROL_PATH);
            if (ctx.monitor_fd >= 0)
                close(ctx.monitor_fd);
            bounded_buffer_destroy(&ctx.log_buffer);
            pthread_mutex_destroy(&ctx.metadata_lock);
            return 1;
        }

        if (listen(ctx.server_fd, 32) < 0) {
            perror("listen");
            close(ctx.server_fd);
            unlink(CONTROL_PATH);
            if (ctx.monitor_fd >= 0)
                close(ctx.monitor_fd);
            bounded_buffer_destroy(&ctx.log_buffer);
            pthread_mutex_destroy(&ctx.metadata_lock);
            return 1;
        }
    }

    if (install_signal_handlers() < 0) {
        perror("sigaction");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    while (!g_supervisor_stop) {
        int client_fd;
        control_request_t req;
        control_response_t resp;

        if (g_supervisor_sigchld) {
            g_supervisor_sigchld = 0;
            reap_children(&ctx);
        }

        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            break;
        }

        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        resp.status = 1;

        if (read(client_fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
            snprintf(resp.message, sizeof(resp.message), "failed to read request");
            (void)write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            continue;
        }

        if (req.kind == CMD_START || req.kind == CMD_RUN) {
            int pipefd[2] = { -1, -1 };
            void *stack = NULL;
            child_config_t cfg;
            pid_t child_pid;
            producer_arg_t *pa;
            producer_handle_t *ph;

            if (req.container_id[0] == '\0' || req.rootfs[0] == '\0' || req.command[0] == '\0') {
                snprintf(resp.message, sizeof(resp.message), "invalid start/run request");
                goto reply;
            }

            pthread_mutex_lock(&ctx.metadata_lock);
            if (find_container_locked(&ctx, req.container_id) != NULL) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                snprintf(resp.message, sizeof(resp.message), "container id already exists");
                goto reply;
            }
            if (rootfs_in_use_locked(&ctx, req.rootfs)) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                snprintf(resp.message, sizeof(resp.message), "rootfs already in use by a running container");
                goto reply;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (pipe(pipefd) < 0) {
                snprintf(resp.message, sizeof(resp.message), "pipe failed: %s", strerror(errno));
                goto reply;
            }

            memset(&cfg, 0, sizeof(cfg));
            strncpy(cfg.id, req.container_id, sizeof(cfg.id) - 1);
            strncpy(cfg.rootfs, req.rootfs, sizeof(cfg.rootfs) - 1);
            strncpy(cfg.command, req.command, sizeof(cfg.command) - 1);
            cfg.nice_value = req.nice_value;
            cfg.log_write_fd = pipefd[1];

            stack = malloc(STACK_SIZE);
            if (!stack) {
                snprintf(resp.message, sizeof(resp.message), "no memory for clone stack");
                close(pipefd[0]);
                close(pipefd[1]);
                goto reply;
            }

            child_pid = clone(child_fn,
                              (char *)stack + STACK_SIZE,
                              CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWNS | SIGCHLD,
                              &cfg);
            close(pipefd[1]);

            if (child_pid < 0) {
                snprintf(resp.message, sizeof(resp.message), "clone failed: %s", strerror(errno));
                free(stack);
                close(pipefd[0]);
                goto reply;
            }

            pthread_mutex_lock(&ctx.metadata_lock);
            if (add_container_locked(&ctx, &req, child_pid) != 0) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                (void)kill(child_pid, SIGKILL);
                (void)waitpid(child_pid, NULL, 0);
                free(stack);
                close(pipefd[0]);
                snprintf(resp.message, sizeof(resp.message), "failed to track container");
                goto reply;
            }
            {
                container_record_t *rec = find_container_locked(&ctx, req.container_id);
                if (rec)
                    rec->child_stack = stack;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (ctx.monitor_fd >= 0) {
                (void)register_with_monitor(ctx.monitor_fd,
                                            req.container_id,
                                            child_pid,
                                            req.soft_limit_bytes,
                                            req.hard_limit_bytes);
            }

            pa = calloc(1, sizeof(*pa));
            ph = calloc(1, sizeof(*ph));
            if (!pa || !ph) {
                free(pa);
                free(ph);
                close(pipefd[0]);
                (void)kill(child_pid, SIGKILL);
                (void)waitpid(child_pid, NULL, 0);
                pthread_mutex_lock(&ctx.metadata_lock);
                {
                    container_record_t *rec = find_container_locked(&ctx, req.container_id);
                    if (rec) {
                        if (ctx.monitor_fd >= 0)
                            (void)unregister_from_monitor(ctx.monitor_fd, rec->id, rec->host_pid);
                        free(rec->child_stack);
                        rec->child_stack = NULL;
                        rec->state = CONTAINER_KILLED;
                        rec->exit_code = 128 + SIGKILL;
                        rec->exit_signal = SIGKILL;
                    }
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
                snprintf(resp.message, sizeof(resp.message), "failed to create logging producer");
                goto reply;
            }

            pa->ctx = &ctx;
            strncpy(pa->container_id, req.container_id, sizeof(pa->container_id) - 1);
            pa->pipe_fd = pipefd[0];

            rc = pthread_create(&ph->tid, NULL, log_producer_thread, pa);
            if (rc != 0) {
                free(pa);
                free(ph);
                close(pipefd[0]);
                (void)kill(child_pid, SIGKILL);
                (void)waitpid(child_pid, NULL, 0);
                pthread_mutex_lock(&ctx.metadata_lock);
                {
                    container_record_t *rec = find_container_locked(&ctx, req.container_id);
                    if (rec) {
                        if (ctx.monitor_fd >= 0)
                            (void)unregister_from_monitor(ctx.monitor_fd, rec->id, rec->host_pid);
                        free(rec->child_stack);
                        rec->child_stack = NULL;
                        rec->state = CONTAINER_KILLED;
                        rec->exit_code = 128 + SIGKILL;
                        rec->exit_signal = SIGKILL;
                    }
                }
                pthread_mutex_unlock(&ctx.metadata_lock);
                snprintf(resp.message, sizeof(resp.message), "pthread_create producer failed");
                goto reply;
            }

            pthread_mutex_lock(&ctx.metadata_lock);
            ph->next = ctx.producers;
            ctx.producers = ph;
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (req.kind == CMD_START) {
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message), "started id=%s pid=%d", req.container_id, child_pid);
                goto reply;
            }

            {
                int status;
                pid_t wp;
                for (;;) {
                    wp = waitpid(child_pid, &status, 0);
                    if (wp < 0 && errno == EINTR)
                        continue;
                    break;
                }

                if (wp == child_pid) {
                    pthread_mutex_lock(&ctx.metadata_lock);
                    container_record_t *rec = find_container_locked(&ctx, req.container_id);
                    if (rec) {
                        rec->run_waiter_active = 0;
                        if (WIFEXITED(status)) {
                            rec->state = CONTAINER_EXITED;
                            rec->exit_code = WEXITSTATUS(status);
                            rec->exit_signal = 0;
                        } else if (WIFSIGNALED(status)) {
                            rec->exit_code = 128 + WTERMSIG(status);
                            rec->exit_signal = WTERMSIG(status);
                            rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_KILLED;
                        }
                        resp.status = rec->exit_code;
                        snprintf(resp.message,
                                 sizeof(resp.message),
                                 "run finished id=%s exit=%d signal=%d",
                                 rec->id,
                                 rec->exit_code,
                                 rec->exit_signal);
                        if (ctx.monitor_fd >= 0)
                            (void)unregister_from_monitor(ctx.monitor_fd, rec->id, rec->host_pid);
                        free(rec->child_stack);
                        rec->child_stack = NULL;
                    } else {
                        resp.status = 1;
                        snprintf(resp.message, sizeof(resp.message), "run container metadata missing");
                    }
                    pthread_mutex_unlock(&ctx.metadata_lock);
                } else {
                    resp.status = 1;
                    snprintf(resp.message, sizeof(resp.message), "waitpid failed for run");
                }
            }

            goto reply;
        }

        if (req.kind == CMD_PS) {
            char buf[CONTROL_MESSAGE_LEN];
            size_t used = 0;

            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *it = ctx.containers;
            if (!it) {
                snprintf(resp.message, sizeof(resp.message), "no containers tracked");
            } else {
                buf[0] = '\0';
                while (it) {
                    int n = snprintf(buf + used,
                                     sizeof(buf) - used,
                                     "%s(pid=%d,state=%s) ",
                                     it->id,
                                     it->host_pid,
                                     state_to_string(it->state));
                    if (n < 0 || (size_t)n >= (sizeof(buf) - used))
                        break;
                    used += (size_t)n;
                    it = it->next;
                }
                snprintf(resp.message, sizeof(resp.message), "%s", buf);
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;
            goto reply;
        }

        if (req.kind == CMD_LOGS) {
            char path[PATH_MAX];
            struct stat st;

            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);
            if (stat(path, &st) == 0) {
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message), "log_path=%s", path);
            } else {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "log not found for id=%s", req.container_id);
            }
            goto reply;
        }

        if (req.kind == CMD_STOP) {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = find_container_locked(&ctx, req.container_id);
            if (!rec) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "container not found");
                goto reply;
            }

            rec->stop_requested = 1;
            if (kill(rec->host_pid, SIGTERM) < 0) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "failed to stop id=%s", req.container_id);
            } else {
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message), "stop signaled id=%s", req.container_id);
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            goto reply;
        }

        snprintf(resp.message, sizeof(resp.message), "unknown command");

reply:
        (void)write(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }

    stop_all_containers(&ctx);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);

    pthread_join(ctx.logger_thread, NULL);

    pthread_mutex_lock(&ctx.metadata_lock);
    {
        producer_handle_t *ph = ctx.producers;
        while (ph) {
            if (!ph->joined)
                pthread_join(ph->tid, NULL);
            ph->joined = 1;
            ph = ph->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    pthread_mutex_lock(&ctx.metadata_lock);
    {
        producer_handle_t *ph = ctx.producers;
        while (ph) {
            producer_handle_t *next = ph->next;
            free(ph);
            ph = next;
        }
        ctx.producers = NULL;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    cleanup_metadata(&ctx);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(fd);
        return 1;
    }

    memset(&resp, 0, sizeof(resp));
    if (read(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        perror("read response");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);

    if (req->kind == CMD_LOGS && resp.status == 0 && strncmp(resp.message, "log_path=", 9) == 0) {
        const char *path = resp.message + 9;
        int log_fd = open(path, O_RDONLY);
        if (log_fd >= 0) {
            char buf[1024];
            ssize_t n;
            while ((n = read(log_fd, buf, sizeof(buf))) > 0)
                (void)write(STDOUT_FILENO, buf, (size_t)n);
            close(log_fd);
        }
    }

    close(fd);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
