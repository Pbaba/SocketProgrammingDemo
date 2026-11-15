/**
 * @file client.c
 * @brief Part B: Full-Duplex Sliding Window Protocol Client.
 *
 * Implements a sliding window client over TCP with a fixed window size of 5.
 * Features separate sender and receiver threads synchronized via a POSIX mutex
 * and condition variable.
 *
 * Concurrency & Protocol Specifications:
 * - Window size: 5
 * - Sender thread transmits "HELLO <seq>\n", blocks when 5 messages are outstanding.
 * - Receiver thread continuously listens for "ACK <seq>\n", validates sequence number,
 *   advances base by 1 for valid in-order ACKs, and signals the sender thread.
 * - Receiver starts before sender.
 * - Sockets are not held under mutex locks.
 * - Safe shutdown and broadcasting ensures no thread deadlocks on server disconnection.
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <ctype.h>
#include <limits.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEFAULT_SERVER_IP "127.0.0.1"
#define DEFAULT_PORT 8080
#define DEFAULT_TOTAL_MESSAGES 15
#define WINDOW_SIZE 5
#define BUFFER_SIZE 1024

/**
 * @brief Shared state for the sliding window protocol.
 */
typedef struct {
    int socket_fd;
    int base;              /* Oldest unacknowledged sequence number */
    int next_seq_num;      /* Next sequence number to be sent */
    int total_messages;    /* Total messages configured to send */
    int window_size;       /* Fixed at 5 */
    int shutdown_or_error; /* Flag indicating socket error or premature server disconnect */
    int completed;         /* Flag indicating all messages sent and acknowledged */
    pthread_mutex_t lock;  /* Protects shared window state */
    pthread_cond_t cond_window; /* Signals sender when window space opens or shutdown occurs */
} SlidingWindowState;

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
 * @return > 0: Valid line length.
 *           0: Peer disconnected cleanly at message boundary.
 *          -1: Socket read error.
 *          -2: Incomplete frame (peer closed connection before sending delimiter).
 */
static int buffered_reader_readline(BufferedReader *reader, char *dest, size_t max_len) {
    size_t dest_idx = 0;

    while (1) {
        if (reader->buf_pos >= reader->buf_len) {
            ssize_t bytes_read = recv(reader->socket_fd, reader->buffer, BUFFER_SIZE, 0);
            if (bytes_read > 0) {
                reader->buf_pos = 0;
                reader->buf_len = (size_t)bytes_read;
            } else if (bytes_read == 0) {
                if (dest_idx > 0) {
                    dest[dest_idx] = '\0';
                    return -2; /* Incomplete frame */
                }
                return 0; /* Clean EOF */
            } else {
                if (errno == EINTR) {
                    continue;
                }
                return -1;
            }
        }

        while (reader->buf_pos < reader->buf_len) {
            char ch = reader->buffer[reader->buf_pos++];
            if (ch == '\n') {
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
 * @brief Sender thread: transmits HELLO messages within the sliding window.
 */
static void *sender_thread_func(void *arg) {
    SlidingWindowState *state = (SlidingWindowState *)arg;
    int was_blocked = 0;

    while (1) {
        pthread_mutex_lock(&state->lock);

        /* Block if the window is currently full */
        while (!state->shutdown_or_error &&
               (state->next_seq_num - state->base >= state->window_size) &&
               (state->next_seq_num <= state->total_messages)) {
            int in_flight = state->next_seq_num - state->base;
            int upper = state->base + state->window_size - 1;
            if (upper > state->total_messages) upper = state->total_messages;

            printf("[Sender] Window full (%d unacknowledged messages, [%d..%d]). Blocking...\n",
                   in_flight, state->base, upper);
            fflush(stdout);
            was_blocked = 1;

            pthread_cond_wait(&state->cond_window, &state->lock);
        }

        /* Check for error or server disconnection */
        if (state->shutdown_or_error) {
            pthread_mutex_unlock(&state->lock);
            break;
        }

        /* Check if all messages have been sent */
        if (state->next_seq_num > state->total_messages) {
            pthread_mutex_unlock(&state->lock);
            break;
        }

        /* If waking up from a blocked state, announce resumption */
        if (was_blocked) {
            printf("[Sender] Window space available. Resuming transmission...\n");
            fflush(stdout);
            was_blocked = 0;
        }

        /* Reserve the next sequence number under lock */
        int seq_to_send = state->next_seq_num++;
        int cur_base = state->base;
        int in_flight = state->next_seq_num - cur_base;
        int upper = cur_base + state->window_size - 1;
        if (upper > state->total_messages) upper = state->total_messages;

        pthread_mutex_unlock(&state->lock);

        /* Format and transmit message outside the lock */
        char send_buf[64];
        int msg_len = snprintf(send_buf, sizeof(send_buf), "HELLO %d\n", seq_to_send);
        if (msg_len <= 0 || (size_t)msg_len >= sizeof(send_buf)) {
            fprintf(stderr, "[Sender] snprintf buffer formatting error\n");
            break;
        }

        if (send_all(state->socket_fd, send_buf, (size_t)msg_len) < 0) {
            fprintf(stderr, "[Sender] Socket write failed for HELLO %d: %s\n",
                    seq_to_send, strerror(errno));
            pthread_mutex_lock(&state->lock);
            state->shutdown_or_error = 1;
            pthread_cond_broadcast(&state->cond_window);
            pthread_mutex_unlock(&state->lock);
            /* Unblock receiver thread if it is waiting in recv() */
            shutdown(state->socket_fd, SHUT_RDWR);
            break;
        }

        printf("[Sender] Sent HELLO %-2d | Window: [%d..%d] | In-flight: %d\n",
               seq_to_send, cur_base, upper, in_flight);
        fflush(stdout);
    }

    printf("[Sender] Sender thread terminated cleanly.\n");
    fflush(stdout);
    return NULL;
}

/**
 * @brief Parses an ACK frame strictly using strtol().
 *
 * Requirements:
 * - Must start with "ACK "
 * - Followed by valid positive digits
 * - Rejects negative numbers and 0
 * - Rejects trailing non-whitespace characters (e.g. "ACK 1xyz")
 * - Handles integer overflow
 *
 * @param line Input line buffer (null-terminated).
 * @param out_seq Output pointer for the parsed sequence number.
 * @return 0 on success, -1 on invalid format / overflow / negative / trailing garbage.
 */
static int parse_ack_message(const char *line, int *out_seq) {
    if (strncmp(line, "ACK ", 4) != 0) {
        return -1;
    }

    const char *p = line + 4;
    while (*p == ' ' || *p == '\t') {
        p++;
    }

    /* Must start with a digit; reject negative signs or non-digits */
    if (!isdigit((unsigned char)*p)) {
        return -1;
    }

    char *endptr = NULL;
    errno = 0;
    long val = strtol(p, &endptr, 10);

    /* Detect conversion error or overflow */
    if (errno == ERANGE || val <= 0 || val > INT_MAX || endptr == p) {
        return -1;
    }

    /* Reject trailing non-whitespace characters (e.g. "ACK 1xyz") */
    while (*endptr != '\0') {
        if (!isspace((unsigned char)*endptr)) {
            return -1;
        }
        endptr++;
    }

    *out_seq = (int)val;
    return 0;
}

/**
 * @brief Receiver thread: continuously reads and processes ACKs from the server.
 */
static void *receiver_thread_func(void *arg) {
    SlidingWindowState *state = (SlidingWindowState *)arg;
    BufferedReader reader;
    buffered_reader_init(&reader, state->socket_fd);
    char recv_line[BUFFER_SIZE];

    while (1) {
        /* Read from socket without holding mutex */
        int read_result = buffered_reader_readline(&reader, recv_line, sizeof(recv_line));

        if (read_result > 0) {
            int ack_seq = 0;
            if (parse_ack_message(recv_line, &ack_seq) != 0) {
                fprintf(stderr, "[Receiver] Malformed or invalid ACK received: \"%s\" (ignored)\n", recv_line);
                continue;
            }

            pthread_mutex_lock(&state->lock);

            /* Validate ACK sequence number */
            if (ack_seq == state->base) {
                /* Valid in-order ACK: advance base by exactly 1 position */
                state->base++;
                int cur_base = state->base;
                int upper = cur_base + state->window_size - 1;
                if (upper > state->total_messages) upper = state->total_messages;
                int in_flight = state->next_seq_num - cur_base;
                if (in_flight < 0) in_flight = 0;

                printf("[Receiver] Received ACK %-2d | Window advanced. New base = %-2d | Window: [%d..%d] | In-flight: %d\n",
                       ack_seq, cur_base, cur_base, upper, in_flight);
                fflush(stdout);

                /* Signal sender thread that window space has opened */
                pthread_cond_signal(&state->cond_window);

                /* Check if all messages have been acknowledged */
                if (state->base > state->total_messages) {
                    state->completed = 1;
                    pthread_cond_broadcast(&state->cond_window);
                    pthread_mutex_unlock(&state->lock);
                    printf("[Receiver] All %d messages have been successfully acknowledged!\n",
                           state->total_messages);
                    fflush(stdout);
                    break;
                }
            } else if (ack_seq < state->base) {
                /* Duplicate ACK */
                fprintf(stderr, "[Receiver] Duplicate ACK %d received (current base=%d, ignored)\n",
                        ack_seq, state->base);
            } else {
                /* Out-of-order or unexpected future ACK */
                fprintf(stderr, "[Receiver] Unexpected out-of-order ACK %d received (expected base=%d, rejected)\n",
                        ack_seq, state->base);
            }

            pthread_mutex_unlock(&state->lock);

        } else if (read_result == 0) {
            /* Server closed connection (clean EOF) */
            pthread_mutex_lock(&state->lock);
            if (!state->completed) {
                printf("[Receiver] Server closed connection prematurely (clean EOF at base=%d, total=%d).\n",
                       state->base, state->total_messages);
                state->shutdown_or_error = 1;
                pthread_cond_broadcast(&state->cond_window);
            }
            pthread_mutex_unlock(&state->lock);
            break;
        } else if (read_result == -2) {
            /* Incomplete frame received on disconnect */
            pthread_mutex_lock(&state->lock);
            fprintf(stderr, "[Receiver] Server disconnected with incomplete frame: \"%s\"\n", recv_line);
            state->shutdown_or_error = 1;
            pthread_cond_broadcast(&state->cond_window);
            pthread_mutex_unlock(&state->lock);
            break;
        } else {
            /* Read error */
            pthread_mutex_lock(&state->lock);
            if (!state->completed) {
                fprintf(stderr, "[Receiver] Socket read error: %s\n", strerror(errno));
                state->shutdown_or_error = 1;
                pthread_cond_broadcast(&state->cond_window);
            }
            pthread_mutex_unlock(&state->lock);
            break;
        }
    }

    printf("[Receiver] Receiver thread terminated cleanly.\n");
    fflush(stdout);
    return NULL;
}

int main(int argc, char *argv[]) {
    const char *server_ip = DEFAULT_SERVER_IP;
    int port = DEFAULT_PORT;
    int total_messages = DEFAULT_TOTAL_MESSAGES;

    if (argc > 1) {
        server_ip = argv[1];
    }
    if (argc > 2) {
        port = atoi(argv[2]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Invalid port number: %s\n", argv[2]);
            return EXIT_FAILURE;
        }
    }
    if (argc > 3) {
        total_messages = atoi(argv[3]);
        if (total_messages <= 0) {
            fprintf(stderr, "Invalid total messages: %s (must be > 0)\n", argv[3]);
            return EXIT_FAILURE;
        }
    }

    signal(SIGPIPE, SIG_IGN);

    /* 1. Create client TCP socket */
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) {
        perror("Socket creation failed");
        return EXIT_FAILURE;
    }

    /* 2. Configure server address */
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)port);

    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid IP address: %s\n", server_ip);
        close(client_fd);
        return EXIT_FAILURE;
    }

    /* 3. Connect to server */
    printf("[Client] Connecting to %s:%d...\n", server_ip, port);
    if (connect(client_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("[Client] Connection to server failed");
        close(client_fd);
        return EXIT_FAILURE;
    }
    printf("[Client] Connected to server at %s:%d\n", server_ip, port);
    printf("[Client] Protocol: Sliding Window | Window Size: %d | Total Messages: %d\n\n",
           WINDOW_SIZE, total_messages);

    /* 4. Initialize shared sliding window state */
    SlidingWindowState state;
    state.socket_fd = client_fd;
    state.base = 1;
    state.next_seq_num = 1;
    state.total_messages = total_messages;
    state.window_size = WINDOW_SIZE;
    state.shutdown_or_error = 0;
    state.completed = 0;

    pthread_mutex_init(&state.lock, NULL);
    pthread_cond_init(&state.cond_window, NULL);

    pthread_t receiver_thread;
    pthread_t sender_thread;

    /* 5. Start receiver thread BEFORE sender thread (Critical Concurrency Requirement) */
    int r_err = pthread_create(&receiver_thread, NULL, receiver_thread_func, &state);
    if (r_err != 0) {
        fprintf(stderr, "Failed to create receiver thread: %s\n", strerror(r_err));
        close(client_fd);
        return EXIT_FAILURE;
    }

    int s_err = pthread_create(&sender_thread, NULL, sender_thread_func, &state);
    if (s_err != 0) {
        fprintf(stderr, "Failed to create sender thread: %s\n", strerror(s_err));
        /* Signal receiver to stop */
        pthread_mutex_lock(&state.lock);
        state.shutdown_or_error = 1;
        pthread_mutex_unlock(&state.lock);
        shutdown(client_fd, SHUT_RDWR);
        pthread_join(receiver_thread, NULL);
        close(client_fd);
        return EXIT_FAILURE;
    }

    /* 6. Wait for both threads to finish cleanly */
    pthread_join(sender_thread, NULL);
    pthread_join(receiver_thread, NULL);

    /* 7. Clean up synchronization primitives and close socket */
    pthread_mutex_destroy(&state.lock);
    pthread_cond_destroy(&state.cond_window);

    printf("\n[Client] Closing socket and exiting...\n");
    close(client_fd);

    if (state.completed) {
        printf("[Client] Run finished: SUCCESS. All %d messages sent and acknowledged.\n", total_messages);
        return EXIT_SUCCESS;
    } else {
        printf("[Client] Run finished: TERMINATED WITH ERRORS.\n");
        return EXIT_FAILURE;
    }
}

