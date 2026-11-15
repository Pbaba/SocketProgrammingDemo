/**
 * @file client.c
 * @brief Part A: TCP Client for Concurrent Client-Server Communication.
 *
 * Connects to the server, repeatedly transmits a "hello\n" message,
 * waits for the corresponding "ACK\n" response, prints the received response,
 * and repeats until terminated.
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEFAULT_SERVER_IP "127.0.0.1"
#define DEFAULT_PORT 8080
#define BUFFER_SIZE 1024
#define SLEEP_INTERVAL_MS 1000

/* Global running flag for clean termination on SIGINT */
static volatile sig_atomic_t g_client_running = 1;

static void handle_sigint(int sig) {
    (void)sig;
    g_client_running = 0;
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
 * @param max_len Size of destination buffer.
 * @return > 0: Length of line read.
 *           0: Peer disconnected cleanly at message boundary (EOF).
 *          -1: Socket read error or buffer overflow.
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
                    /* Incomplete frame: peer disconnected without newline delimiter */
                    dest[dest_idx] = '\0';
                    return -2;
                }
                return 0; /* Clean EOF at frame boundary */
            } else {
                if (errno == EINTR) {
                    if (!g_client_running) {
                        return 0;
                    }
                    continue;
                }
                return -1; /* Socket read error */
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
                if (!g_client_running) {
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
 * @brief Helper function to sleep for a specified number of milliseconds.
 */
static void sleep_ms(int ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

int main(int argc, char *argv[]) {
    const char *server_ip = DEFAULT_SERVER_IP;
    int port = DEFAULT_PORT;
    int message_count = 0; /* 0 means repeat indefinitely until terminated */

    if (argc > 1) {
        server_ip = argv[1];
    }
    if (argc > 2) {
        port = atoi(argv[2]);
        if (port <= 0 || port > 65535) {
            fprintf(stderr, "Invalid port: %s\n", argv[2]);
            return EXIT_FAILURE;
        }
    }
    if (argc > 3) {
        message_count = atoi(argv[3]);
        if (message_count < 0) {
            fprintf(stderr, "Invalid message count: %s\n", argv[3]);
            return EXIT_FAILURE;
        }
    }

    /* Ignore SIGPIPE and handle SIGINT for graceful exit */
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);

    /* 1. Create client socket */
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) {
        perror("Failed to create socket");
        return EXIT_FAILURE;
    }

    /* 2. Configure server address structure */
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)port);

    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid address / Address not supported: %s\n", server_ip);
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
    printf("[Client] Connected successfully to server at %s:%d\n", server_ip, port);
    if (message_count > 0) {
        printf("[Client] Configured to send %d message(s).\n", message_count);
    } else {
        printf("[Client] Configured to repeat indefinitely. Press Ctrl+C to terminate.\n");
    }

    BufferedReader reader;
    buffered_reader_init(&reader, client_fd);

    const char *hello_msg = "hello\n";
    size_t hello_len = strlen(hello_msg);
    char ack_buffer[BUFFER_SIZE];
    int exchange_index = 0;

    /* 4. Message exchange loop: send "hello\n", await "ACK\n" */
    while (g_client_running) {
        exchange_index++;

        /* Send hello message */
        if (send_all(client_fd, hello_msg, hello_len) < 0) {
            fprintf(stderr, "[Client] Failed to send message: %s\n", strerror(errno));
            break;
        }
        printf("[Client] [#%d] Sent: \"hello\"\n", exchange_index);
        fflush(stdout);

        /* Wait for corresponding ACK response */
        int read_result = buffered_reader_readline(&reader, ack_buffer, sizeof(ack_buffer));
        if (read_result > 0) {
            printf("[Client] [#%d] Received: \"%s\"\n", exchange_index, ack_buffer);
            fflush(stdout);

            /* Validate that the response is indeed "ACK" */
            if (strcmp(ack_buffer, "ACK") != 0) {
                fprintf(stderr, "[Client] [#%d] Warning: Unexpected response: \"%s\"\n",
                        exchange_index, ack_buffer);
            }
        } else if (read_result == 0) {
            printf("[Client] Server closed connection (clean EOF).\n");
            break;
        } else if (read_result == -2) {
            fprintf(stderr, "[Client] Server closed connection with incomplete frame: \"%s\" (discarded).\n",
                    ack_buffer);
            break;
        } else {
            fprintf(stderr, "[Client] Error receiving response: %s\n", strerror(errno));
            break;
        }

        /* Check if target message count reached */
        if (message_count > 0 && exchange_index >= message_count) {
            printf("[Client] Successfully completed %d exchanges.\n", message_count);
            break;
        }

        /* Interval delay before next exchange */
        sleep_ms(SLEEP_INTERVAL_MS);
    }

    /* Clean resource teardown */
    printf("[Client] Closing socket and exiting...\n");
    close(client_fd);
    return EXIT_SUCCESS;
}
