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

typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} log_producer_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

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

void *log_producer_thread(void *arg)
{
    log_producer_config_t *config = arg;
    char chunk[LOG_CHUNK_SIZE];

    if (!config)
        return NULL;

    if (config->read_fd < 0 || !config->buffer) {
        free(config);
        return NULL;
    }

    for (;;) {
        ssize_t nread = read(config->read_fd, chunk, sizeof(chunk));

        if (nread < 0) {
            if (errno == EINTR)
                continue;
            perror("read container log pipe");
            break;
        }

        if (nread == 0)
            break;

        log_item_t item;

        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, config->container_id, sizeof(item.container_id) - 1);
        item.length = (size_t)nread;
        memcpy(item.data, chunk, item.length);

        if (bounded_buffer_push(config->buffer, &item) != 0)
            break;
    }

    close(config->read_fd);
    free(config);
    return NULL;
}

static int write_all(int fd, const char *data, size_t length)
{
    size_t written = 0;

    while (written < length) {
        ssize_t rc = write(fd, data + written, length - written);

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (rc == 0) {
            errno = EIO;
            return -1;
        }

        written += (size_t)rc;
    }

    return 0;
}

static int ensure_log_dir(void)
{
    struct stat st;

    if (mkdir(LOG_DIR, 0755) == 0)
        return 0;

    if (errno != EEXIST)
        return -1;

    if (stat(LOG_DIR, &st) != 0)
        return -1;

    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }

    return 0;
}

static int is_safe_log_id(const char *id)
{
    size_t i;

    if (!id || id[0] == '\0')
        return 0;

    for (i = 0; id[i] != '\0'; i++) {
        if ((id[i] >= 'a' && id[i] <= 'z') ||
            (id[i] >= 'A' && id[i] <= 'Z') ||
            (id[i] >= '0' && id[i] <= '9') ||
            id[i] == '_' ||
            id[i] == '-' ||
            id[i] == '.')
            continue;
        return 0;
    }

    return 1;
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
    supervisor_ctx_t *ctx = arg;
    bounded_buffer_t *buffer;
    log_item_t item;

    if (!ctx)
        return NULL;
    buffer = &ctx->log_buffer;

    if (ensure_log_dir() != 0) {
        perror("mkdir logs");
        return NULL;
    }

    while (bounded_buffer_pop(buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        if (!is_safe_log_id(item.container_id)) {
            fprintf(stderr, "Unsafe log container id: %s\n", item.container_id);
            continue;
        }

        if (snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id) >=
            (int)sizeof(path)) {
            fprintf(stderr, "Log path too long for container: %s\n", item.container_id);
            continue;
        }

        fd = open(path, O_CREAT | O_WRONLY | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0644);
        if (fd < 0) {
            perror("open container log");
            continue;
        }

        if (write_all(fd, item.data, item.length) != 0)
            perror("write container log");

        if (close(fd) != 0)
            perror("close container log");
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
    child_config_t *config = arg;
    char *child_argv[2];

    if (!config) {
        fprintf(stderr, "Missing child configuration\n");
        return 1;
    }

    if (sethostname(config->id, strlen(config->id)) != 0) {
        perror("sethostname");
        return 1;
    }

    if (chroot(config->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        return 1;
    }

    if (dup2(config->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        return 1;
    }

    if (dup2(config->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        return 1;
    }

    close(config->log_write_fd);

    if (config->nice_value != 0) {
        errno = 0;
        if (nice(config->nice_value) == -1 && errno != 0) {
            perror("nice");
            return 1;
        }
    }

    child_argv[0] = config->command;
    child_argv[1] = NULL;
    execvp(child_argv[0], child_argv);
    perror("execvp");
    return 1;
}

pid_t create_container_process(supervisor_ctx_t *ctx,
                               const control_request_t *req,
                               pthread_t *producer_thread)
{
    child_config_t *child_config = NULL;
    log_producer_config_t *producer_config = NULL;
    void *child_stack = NULL;
    int pipe_fds[2] = {-1, -1};
    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t child_pid;
    pthread_t thread;

    if (!ctx || !req) {
        errno = EINVAL;
        return -1;
    }

    if (pipe(pipe_fds) != 0)
        return -1;

    child_config = calloc(1, sizeof(*child_config));
    if (!child_config)
        goto fail;

    producer_config = calloc(1, sizeof(*producer_config));
    if (!producer_config)
        goto fail;

    child_stack = malloc(STACK_SIZE);
    if (!child_stack)
        goto fail;

    strncpy(child_config->id, req->container_id, sizeof(child_config->id) - 1);
    strncpy(child_config->rootfs, req->rootfs, sizeof(child_config->rootfs) - 1);
    strncpy(child_config->command, req->command, sizeof(child_config->command) - 1);
    child_config->nice_value = req->nice_value;
    child_config->log_write_fd = pipe_fds[1];

    producer_config->read_fd = pipe_fds[0];
    strncpy(producer_config->container_id,
            req->container_id,
            sizeof(producer_config->container_id) - 1);
    producer_config->buffer = &ctx->log_buffer;

    child_pid = clone(child_fn, (char *)child_stack + STACK_SIZE, flags, child_config);
    if (child_pid < 0)
        goto fail;

    close(pipe_fds[1]);
    pipe_fds[1] = -1;

    if (pthread_create(&thread, NULL, log_producer_thread, producer_config) != 0) {
        kill(child_pid, SIGTERM);
        goto fail;
    }

    if (producer_thread)
        *producer_thread = thread;
    else
        pthread_detach(thread);

    free(child_config);
    free(child_stack);
    return child_pid;

fail:
    if (pipe_fds[0] >= 0)
        close(pipe_fds[0]);
    if (pipe_fds[1] >= 0)
        close(pipe_fds[1]);
    free(producer_config);
    free(child_config);
    free(child_stack);
    return -1;
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

static volatile sig_atomic_t supervisor_got_sigchld;
static volatile sig_atomic_t supervisor_should_shutdown;

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGCHLD)
        supervisor_got_sigchld = 1;
    else if (signo == SIGINT || signo == SIGTERM)
        supervisor_should_shutdown = 1;
}

static int install_supervisor_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGCHLD, &sa, NULL) != 0)
        return -1;
    if (sigaction(SIGINT, &sa, NULL) != 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) != 0)
        return -1;

    return 0;
}

static int read_control_request(int fd, control_request_t *req)
{
    size_t received = 0;
    char *cursor = (char *)req;

    while (received < sizeof(*req)) {
        ssize_t rc = read(fd, cursor + received, sizeof(*req) - received);

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (rc == 0)
            return -1;

        received += (size_t)rc;
    }

    return 0;
}

static int read_control_response(int fd, control_response_t *resp)
{
    size_t received = 0;
    char *cursor = (char *)resp;

    while (received < sizeof(*resp)) {
        ssize_t rc = read(fd, cursor + received, sizeof(*resp) - received);

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (rc == 0)
            return -1;

        received += (size_t)rc;
    }

    return 0;
}

static int write_control_response(int fd, const control_response_t *resp)
{
    return write_all(fd, (const char *)resp, sizeof(*resp));
}

static container_record_t *find_container_by_id(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *record;

    for (record = ctx->containers; record; record = record->next) {
        if (strncmp(record->id, id, sizeof(record->id)) == 0)
            return record;
    }

    return NULL;
}

static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *record;

    for (record = ctx->containers; record; record = record->next) {
        if (record->host_pid == pid)
            return record;
    }

    return NULL;
}

static void reap_exited_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    for (;;) {
        pid = waitpid(-1, &status, WNOHANG);
        if (pid <= 0)
            break;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *record = find_container_by_pid(ctx, pid);
        if (record) {
            if (WIFEXITED(status)) {
                record->exit_code = WEXITSTATUS(status);
                record->exit_signal = 0;
                if (record->state != CONTAINER_STOPPED)
                    record->state = CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                record->exit_code = -1;
                record->exit_signal = WTERMSIG(status);
                if (record->state != CONTAINER_STOPPED)
                    record->state = CONTAINER_KILLED;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static int add_container_record(supervisor_ctx_t *ctx,
                                const control_request_t *req,
                                pid_t host_pid)
{
    container_record_t *record = calloc(1, sizeof(*record));

    if (!record)
        return -1;

    strncpy(record->id, req->container_id, sizeof(record->id) - 1);
    record->host_pid = host_pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->exit_code = -1;
    record->exit_signal = 0;
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);
    return 0;
}

static void handle_start_request(supervisor_ctx_t *ctx,
                                 const control_request_t *req,
                                 control_response_t *resp)
{
    pid_t pid;

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "container already exists: %s\n", req->container_id);
        return;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    pid = create_container_process(ctx, req, NULL);
    if (pid < 0) {
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "failed to start container %s: %s\n",
                 req->container_id, strerror(errno));
        return;
    }

    if (add_container_record(ctx, req, pid) != 0) {
        kill(pid, SIGTERM);
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "failed to record container %s\n",
                 req->container_id);
        return;
    }

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message), "started %s pid=%d\n", req->container_id, pid);
}

static void handle_ps_request(supervisor_ctx_t *ctx, control_response_t *resp)
{
    size_t used;
    container_record_t *record;

    resp->status = 0;
    used = (size_t)snprintf(resp->message, sizeof(resp->message), "ID PID STATE EXIT SIGNAL\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record && used < sizeof(resp->message); record = record->next) {
        int rc = snprintf(resp->message + used,
                          sizeof(resp->message) - used,
                          "%s %d %s %d %d\n",
                          record->id,
                          record->host_pid,
                          state_to_string(record->state),
                          record->exit_code,
                          record->exit_signal);
        if (rc < 0)
            break;
        if ((size_t)rc >= sizeof(resp->message) - used) {
            used = sizeof(resp->message);
            break;
        }
        used += (size_t)rc;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void handle_stop_request(supervisor_ctx_t *ctx,
                                const control_request_t *req,
                                control_response_t *resp)
{
    pid_t pid = -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *record = find_container_by_id(ctx, req->container_id);
    if (record) {
        pid = record->host_pid;
        record->state = CONTAINER_STOPPED;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pid <= 0) {
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "container not found: %s\n", req->container_id);
        return;
    }

    if (kill(pid, SIGTERM) != 0 && errno != ESRCH) {
        resp->status = 1;
        snprintf(resp->message, sizeof(resp->message), "failed to stop %s: %s\n",
                 req->container_id, strerror(errno));
        return;
    }

    usleep(200000);
    reap_exited_children(ctx);

    if (kill(pid, 0) == 0)
        kill(pid, SIGKILL);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message), "stopped %s\n", req->container_id);
}

static void handle_client_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;

    memset(&req, 0, sizeof(req));
    memset(&resp, 0, sizeof(resp));

    if (read_control_request(client_fd, &req) != 0) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "failed to read request\n");
        write_control_response(client_fd, &resp);
        return;
    }

    switch (req.kind) {
    case CMD_START:
        handle_start_request(ctx, &req, &resp);
        break;
    case CMD_PS:
        handle_ps_request(ctx, &resp);
        break;
    case CMD_STOP:
        handle_stop_request(ctx, &req, &resp);
        break;
    default:
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "unsupported command in supervisor core\n");
        break;
    }

    write_control_response(client_fd, &resp);
}

static void terminate_running_containers(supervisor_ctx_t *ctx)
{
    container_record_t *record;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record; record = record->next) {
        if (record->state == CONTAINER_RUNNING || record->state == CONTAINER_STARTING) {
            kill(record->host_pid, SIGTERM);
            record->state = CONTAINER_STOPPED;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    usleep(200000);
    reap_exited_children(ctx);

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record; record = record->next) {
        if (kill(record->host_pid, 0) == 0)
            kill(record->host_pid, SIGKILL);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void free_container_records(supervisor_ctx_t *ctx)
{
    container_record_t *record = ctx->containers;

    while (record) {
        container_record_t *next = record->next;
        free(record);
        record = next;
    }
    ctx->containers = NULL;
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
    struct sockaddr_un addr;
    int rc;
    int logger_started = 0;
    int exit_status = 0;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    (void)rootfs;

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

    if (install_supervisor_signal_handlers() != 0) {
        perror("sigaction");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }
    logger_started = 1;

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        ctx.should_stop = 1;
        exit_status = 1;
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    unlink(CONTROL_PATH);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind control socket");
        ctx.should_stop = 1;
        exit_status = 1;
        goto cleanup;
    }

    if (listen(ctx.server_fd, 16) != 0) {
        perror("listen control socket");
        ctx.should_stop = 1;
        exit_status = 1;
        goto cleanup;
    }

    fprintf(stderr, "Supervisor listening on %s\n", CONTROL_PATH);

    while (!ctx.should_stop && !supervisor_should_shutdown) {
        int client_fd;

        if (supervisor_got_sigchld) {
            supervisor_got_sigchld = 0;
            reap_exited_children(&ctx);
        }

        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept control socket");
            exit_status = 1;
            break;
        }

        handle_client_request(&ctx, client_fd);
        close(client_fd);

        if (supervisor_got_sigchld) {
            supervisor_got_sigchld = 0;
            reap_exited_children(&ctx);
        }
    }

cleanup:
    terminate_running_containers(&ctx);
    reap_exited_children(&ctx);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (logger_started)
        pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);
    free_container_records(&ctx);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return exit_status;
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
    struct sockaddr_un addr;
    control_response_t resp;
    int fd;
    int status = 1;

    if (!req) {
        fprintf(stderr, "Missing control request\n");
        return 1;
    }

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        fprintf(stderr, "Could not connect to supervisor at %s: %s\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    if (write_all(fd, (const char *)req, sizeof(*req)) != 0) {
        fprintf(stderr, "Failed to send request to supervisor: %s\n", strerror(errno));
        close(fd);
        return 1;
    }

    memset(&resp, 0, sizeof(resp));
    if (read_control_response(fd, &resp) != 0) {
        fprintf(stderr, "Failed to read response from supervisor: %s\n", strerror(errno));
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0')
        fputs(resp.message, stdout);

    status = resp.status;
    close(fd);
    return status;
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

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
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
