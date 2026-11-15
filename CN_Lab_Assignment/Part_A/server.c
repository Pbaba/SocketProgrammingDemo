/**
 * @file server.c
 * @brief Part A: Concurrent TCP Server using POSIX Threads (pthreads).
 *
 * Implements a multi-threaded TCP server that listens for incoming client
 * connections. For each client, a dedicated POSIX thread is spawned to handle
 * communication independently.
 *
 * Key engineering features:
 * - Async-signal-safe shutdown via self-pipe trick.
 * - Thread-safe shutdown state protected by mutex.
 * - Active client worker thread tracking with condition variable.
 * - Full verification of pthread return values.
 * - Prevention of incomplete newline-delimited TCP frames being processed on disconnect.
 * - Independent handling of unexpected client disconnections.
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEFAULT_PORT 8080
#define BUFFER_SIZE 1024
#define LISTEN_BACKLOG 10

/* Async-signal-safe self-pipe for shutdown notifications */
static int g_shutdown_pipe[2] = {-1, -1};

/* Thread-safe shutdown state protected by mutex */
static pthread_mutex_t g_shutdown_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_server_shutting_down = 0;

/* Mutex for thread-safe console logging and client ID generation */
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_client_counter = 0;

/**
 * @brief Structure for tracking active client worker threads.
 */
typedef struct ClientContext {
    int client_fd;
    struct sockaddr_in client_addr;
    int client_id;
    struct ClientContext *prev;
    struct ClientContext *next;
} ClientContext;

/* Active client list and synchronization */
static pthread_mutex_t g_clients_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_clients_done_cv = PTHREAD_COND_INITIALIZER;
static ClientContext *g_client_list_head = NULL;
static int g_active_clients = 0;

/**
 * @brief Thread-safe check of shutdown flag.
 */
static int is_server_shutting_down(void) {
    pthread_mutex_lock(&g_shutdown_mutex);
    int status = g_server_shutting_down;
    pthread_mutex_unlock(&g_shutdown_mutex);
    return status;
}

/**
 * @brief Thread-safe setter for shutdown flag.
 */
static void set_server_shutting_down(int status) {
    pthread_mutex_lock(&g_shutdown_mutex);
    g_server_shutting_down = status;
    pthread_mutex_unlock(&g_shutdown_mutex);
}

/**
 * @brief Async-signal-safe signal handler.
 * Only writes 1 byte to the self-pipe; performs no locks or unsafe I/O.
 */
static void handle_shutdown_signal(int sig) {
    (void)sig;
    if (g_shutdown_pipe[1] >= 0) {
        char notify_byte = 'S';
        ssize_t res = write(g_shutdown_pipe[1], &notify_byte, 1);
        (void)res;
    }
}

/**
 * @brief Buffered reader structure to handle TCP stream framing.
 */
typedef struct {
    int socket_fd;
    char buffer[BUFFER_SIZE];
    size_t buf_pos;
    size_t buf_len;
} BufferedReader;

static void buffered_reader_init(BufferedReader *reader, int socket_fd) {
    reader->socket_fd = socket_fd;
    reader->buf_pos = 0;
    reader->buf_len = 0;
}

/**
 * @brief Reads a single newline-delimited line from the socket.
 *
 * @param reader Pointer to initialized BufferedReader.
 * @param dest Output buffer for the extracted line (null-terminated).
 * @param max_len Size of the destination buffer.
 * @return > 0: Length of valid complete line read (excluding delimiter).
 *           0: Peer disconnected cleanly at message boundary (EOF).
 *          -1: Socket read error or buffer overflow.
 *          -2: Incomplete frame (peer closed connection before sending delimiter).
 */
static int buffered_reader_readline(BufferedReader *reader, char *dest, size_t max_len) {
    size_t dest_idx = 0;

    while (1) {
        /* If internal buffer is exhausted, fetch more data from socket */
        if (reader->buf_pos >= reader->buf_len) {
            ssize_t bytes_read = recv(reader->socket_fd, reader->buffer, BUFFER_SIZE, 0);
            if (bytes_read > 0) {
                reader->buf_pos = 0;
                reader->buf_len = (size_t)bytes_read;
            } else if (bytes_read == 0) {
                /* Connection closed by peer */
                if (dest_idx > 0) {
                    /* Incomplete frame: peer disconnected without trailing newline */
                    dest[dest_idx] = '\0';
                    return -2;
                }
                return 0; /* Clean EOF at frame boundary */
            } else {
                if (errno == EINTR) {
                    if (is_server_shutting_down()) {
                        return -1;
                    }
                    continue; /* Retry if interrupted by signal */
                }
                return -1; /* Socket read error */
            }
        }

        /* Scan through buffered bytes for newline delimiter */
        while (reader->buf_pos < reader->buf_len) {
            char ch = reader->buffer[reader->buf_pos++];
            if (ch == '\n') {
                /* Strip optional carriage return if present */
                if (dest_idx > 0 && dest[dest_idx - 1] == '\r') {
                    dest[dest_idx - 1] = '\0';
                    return (int)(dest_idx - 1);
                }
                dest[dest_idx] = '\0';
                return (int)dest_idx;
            }

            if (dest_idx + 1 < max_len) {
                dest[dest_idx++] = ch;
            } else {
                /* Frame exceeds destination buffer size */
                dest[dest_idx] = '\0';
                return -1;
            }
        }
    }
}

/**
 * @brief Sends exactly len bytes over a socket, handling partial writes.
 */
static int send_all(int socket_fd, const char *buf, size_t len) {
    size_t total_sent = 0;
    while (total_sent < len) {
        ssize_t bytes_sent = send(socket_fd, buf + total_sent, len - total_sent, 0);
        if (bytes_sent < 0) {
            if (errno == EINTR) {
                if (is_server_shutting_down()) {
                    return -1;
                }
                continue;
            }
            return -1;
        }
        if (bytes_sent == 0) {
            return -1;
        }
        total_sent += (size_t)bytes_sent;
    }
    return 0;
}

/**
 * @brief Worker thread entry point for handling an individual connected client.
 */
static void *client_worker_thread(void *arg) {
    ClientContext *ctx = (ClientContext *)arg;
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(ctx->client_addr.sin_addr), client_ip, INET_ADDRSTRLEN);
    int client_port = ntohs(ctx->client_addr.sin_port);

    pthread_mutex_lock(&g_log_mutex);
    printf("[Server] [Client #%d] Connected from %s:%d (Thread ID: %lu)\n",
           ctx->client_id, client_ip, client_port, (unsigned long)pthread_self());
    fflush(stdout);
    pthread_mutex_unlock(&g_log_mutex);

    BufferedReader reader;
    buffered_reader_init(&reader, ctx->client_fd);
    char recv_line[BUFFER_SIZE];
    const char *ack_response = "ACK\n";
    size_t ack_len = strlen(ack_response);

    while (!is_server_shutting_down()) {
        int read_result = buffered_reader_readline(&reader, recv_line, sizeof(recv_line));

        if (read_result > 0) {
            pthread_mutex_lock(&g_log_mutex);
            printf("[Server] [Client #%d] Received: \"%s\"\n", ctx->client_id, recv_line);
            fflush(stdout);
            pthread_mutex_unlock(&g_log_mutex);

            /* Respond with ACK */
            if (send_all(ctx->client_fd, ack_response, ack_len) < 0) {
                if (!is_server_shutting_down()) {
                    pthread_mutex_lock(&g_log_mutex);
                    fprintf(stderr, "[Server] [Client #%d] Failed to send ACK: %s\n",
                            ctx->client_id, strerror(errno));
                    fflush(stderr);
                    pthread_mutex_unlock(&g_log_mutex);
                }
                break;
            }

            pthread_mutex_lock(&g_log_mutex);
            printf("[Server] [Client #%d] Sent: \"ACK\"\n", ctx->client_id);
            fflush(stdout);
            pthread_mutex_unlock(&g_log_mutex);
        } else if (read_result == 0) {
            /* Client closed connection cleanly at message boundary */
            pthread_mutex_lock(&g_log_mutex);
            printf("[Server] [Client #%d] Client disconnected gracefully.\n", ctx->client_id);
            fflush(stdout);
            pthread_mutex_unlock(&g_log_mutex);
            break;
        } else if (read_result == -2) {
            /* Client dropped with incomplete/un-delimited frame */
            pthread_mutex_lock(&g_log_mutex);
            fprintf(stderr, "[Server] [Client #%d] Disconnected with incomplete frame: \"%s\" (discarded)\n",
                    ctx->client_id, recv_line);
            fflush(stderr);
            pthread_mutex_unlock(&g_log_mutex);
            break;
        } else {
            /* Read error or unexpected disconnection */
            if (!is_server_shutting_down()) {
                pthread_mutex_lock(&g_log_mutex);
                fprintf(stderr, "[Server] [Client #%d] Unexpected disconnection / read error: %s\n",
                        ctx->client_id, strerror(errno));
                fflush(stderr);
                pthread_mutex_unlock(&g_log_mutex);
            }
            break;
        }
    }

    /* Unlink client from active client tracking list */
    pthread_mutex_lock(&g_clients_mutex);
    if (ctx->prev) {
        ctx->prev->next = ctx->next;
    } else {
        g_client_list_head = ctx->next;
    }
    if (ctx->next) {
        ctx->next->prev = ctx->prev;
    }
    g_active_clients--;

    pthread_mutex_lock(&g_log_mutex);
    printf("[Server] [Client #%d] Cleaned up worker thread (Remaining active clients: %d)\n",
           ctx->client_id, g_active_clients);
    fflush(stdout);
    pthread_mutex_unlock(&g_log_mutex);

    if (g_active_clients == 0) {
        pthread_cond_broadcast(&g_clients_done_cv);
    }
    pthread_mutex_unlock(&g_clients_mutex);

    /* Close client socket and free context memory */
    close(ctx->client_fd);
    free(ctx);
    return NULL;
}

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    if (argc > 1) {
        port = atoi(argv[1]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Invalid port number: %s (must be 1-65535)\n", argv[1]);
            return EXIT_FAILURE;
        }
    }

    /* Ignore SIGPIPE so writes on broken sockets return EPIPE rather than killing process */
    signal(SIGPIPE, SIG_IGN);

    /* 1. Create non-blocking self-pipe for async-signal-safe shutdown */
    if (pipe(g_shutdown_pipe) < 0) {
        perror("Failed to create shutdown pipe");
        return EXIT_FAILURE;
    }
    int flags = fcntl(g_shutdown_pipe[0], F_GETFL, 0);
    fcntl(g_shutdown_pipe[0], F_SETFL, flags | O_NONBLOCK);
    flags = fcntl(g_shutdown_pipe[1], F_GETFL, 0);
    fcntl(g_shutdown_pipe[1], F_SETFL, flags | O_NONBLOCK);

    /* Register shutdown signal handlers */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) < 0 || sigaction(SIGTERM, &sa, NULL) < 0) {
        perror("sigaction registration failed");
        close(g_shutdown_pipe[0]);
        close(g_shutdown_pipe[1]);
        return EXIT_FAILURE;
    }

    /* 2. Create TCP listening socket */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("Failed to create socket");
        close(g_shutdown_pipe[0]);
        close(g_shutdown_pipe[1]);
        return EXIT_FAILURE;
    }

    /* Enable SO_REUSEADDR */
    int reuse_opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_opt, sizeof(reuse_opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        close(listen_fd);
        close(g_shutdown_pipe[0]);
        close(g_shutdown_pipe[1]);
        return EXIT_FAILURE;
    }

    /* 3. Bind socket */
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons((uint16_t)port);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Failed to bind socket");
        close(listen_fd);
        close(g_shutdown_pipe[0]);
        close(g_shutdown_pipe[1]);
        return EXIT_FAILURE;
    }

    /* 4. Listen */
    if (listen(listen_fd, LISTEN_BACKLOG) < 0) {
        perror("Failed to listen on socket");
        close(listen_fd);
        close(g_shutdown_pipe[0]);
        close(g_shutdown_pipe[1]);
        return EXIT_FAILURE;
    }

    printf("====================================================\n");
    printf(" Part A Concurrent TCP Server running on port %d\n", port);
    printf(" Press Ctrl+C to terminate cleanly.\n");
    printf("====================================================\n");
    fflush(stdout);

    /* 5. Main event loop using poll() to multiplex listening socket and shutdown pipe */
    struct pollfd poll_fds[2];
    poll_fds[0].fd = listen_fd;
    poll_fds[0].events = POLLIN;
    poll_fds[1].fd = g_shutdown_pipe[0];
    poll_fds[1].events = POLLIN;

    while (!is_server_shutting_down()) {
        int poll_res = poll(poll_fds, 2, -1);
        if (poll_res < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("poll() failed");
            break;
        }

        /* Check for shutdown signal notification */
        if (poll_fds[1].revents & POLLIN) {
            char dump[32];
            while (read(g_shutdown_pipe[0], dump, sizeof(dump)) > 0);
            set_server_shutting_down(1);
            break;
        }

        /* Check for incoming client connection */
        if (poll_fds[0].revents & POLLIN) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);

            int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
            if (client_fd < 0) {
                if (errno == EINTR) {
                    continue;
                }
                perror("accept() failed");
                continue;
            }

            /* Allocate context for client worker thread */
            ClientContext *ctx = (ClientContext *)malloc(sizeof(ClientContext));
            if (!ctx) {
                perror("malloc for ClientContext failed");
                close(client_fd);
                continue;
            }

            ctx->client_fd = client_fd;
            ctx->client_addr = client_addr;

            /* Register client in tracked active client list */
            pthread_mutex_lock(&g_clients_mutex);
            ctx->client_id = ++g_client_counter;
            ctx->prev = NULL;
            ctx->next = g_client_list_head;
            if (g_client_list_head) {
                g_client_list_head->prev = ctx;
            }
            g_client_list_head = ctx;
            g_active_clients++;
            pthread_mutex_unlock(&g_clients_mutex);

            /* Spawn dedicated worker POSIX thread */
            pthread_t worker_thread;
            int pt_err = pthread_create(&worker_thread, NULL, client_worker_thread, ctx);
            if (pt_err != 0) {
                fprintf(stderr, "pthread_create failed: %s\n", strerror(pt_err));
                /* Rollback client registration */
                pthread_mutex_lock(&g_clients_mutex);
                if (ctx->prev) {
                    ctx->prev->next = ctx->next;
                } else {
                    g_client_list_head = ctx->next;
                }
                if (ctx->next) {
                    ctx->next->prev = ctx->prev;
                }
                g_active_clients--;
                pthread_mutex_unlock(&g_clients_mutex);

                close(client_fd);
                free(ctx);
                continue;
            }

            /* Detach thread and strictly verify return value */
            int detach_err = pthread_detach(worker_thread);
            if (detach_err != 0) {
                fprintf(stderr, "pthread_detach failed for Client #%d: %s\n",
                        ctx->client_id, strerror(detach_err));
            }
        }
    }

    printf("\n[Server] Shutdown initiated. Closing listening socket...\n");
    close(listen_fd);

    /* 6. Gracefully shut down and wait for all active client worker threads */
    pthread_mutex_lock(&g_clients_mutex);
    if (g_active_clients > 0) {
        printf("[Server] Notifying %d active client connection(s) to terminate...\n", g_active_clients);
        ClientContext *curr = g_client_list_head;
        while (curr) {
            /* shutdown() unblocks any pending recv() in the worker thread */
            shutdown(curr->client_fd, SHUT_RDWR);
            curr = curr->next;
        }

        while (g_active_clients > 0) {
            printf("[Server] Waiting for %d active worker thread(s) to finish...\n", g_active_clients);
            pthread_cond_wait(&g_clients_done_cv, &g_clients_mutex);
        }
    }
    pthread_mutex_unlock(&g_clients_mutex);

    printf("[Server] All worker threads cleanly terminated.\n");

    /* Close shutdown pipe */
    close(g_shutdown_pipe[0]);
    close(g_shutdown_pipe[1]);

    printf("[Server] Server process terminated cleanly.\n");
    return EXIT_SUCCESS;
}
