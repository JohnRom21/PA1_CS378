/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/*
Please specify the group members here
yehonatan rom
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
    int epoll_fd;        /* File descriptor for the epoll instance, used for monitoring events on the socket. */
    int socket_fd;       /* File descriptor for the client socket connected to the server. */
    long long total_rtt; /* Accumulated Round-Trip Time (RTT) for all messages sent and received (in microseconds). */
    long total_messages; /* Total number of messages sent and received. */
    float request_rate;  /* Computed request rate (requests per second) based on RTT and total messages. */
} client_thread_data_t;

/* ------------------------- Helpers ------------------------- */

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return -1;
    }
    return 0;
}

static long long tv_to_us(const struct timeval *tv) {
    return (long long)tv->tv_sec * 1000000LL + (long long)tv->tv_usec;
}

static long long tv_diff_us(const struct timeval *start, const struct timeval *end) {
    return tv_to_us(end) - tv_to_us(start);
}

static int send_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t left = len;

    while (left > 0) {
        ssize_t n = send(fd, p, left, 0);
        if (n > 0) {
            p += (size_t)n;
            left -= (size_t)n;
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            continue;
        }
        return -1;
    }
    return 0;
}

/* read as much as available (nonblocking); returns:
 *  >0 bytes read
 *   0 peer closed
 *  -1 error (other than EAGAIN/EINTR)
 *  -2 would block (EAGAIN/EWOULDBLOCK)
 */
static ssize_t recv_some(int fd, void *buf, size_t len) {
    ssize_t n = recv(fd, buf, len, 0);
    if (n > 0) return n;
    if (n == 0) return 0;
    if (errno == EINTR) return -2;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return -2;
    return -1;
}

/* ------------------------- Client ------------------------- */

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event, events[MAX_EVENTS];
    double total_rtt_s = 0.0;

    /* Send 16-Bytes message every time (no trailing '\0' needed) */
    char send_buf[MESSAGE_SIZE];
    memcpy(send_buf, "ABCDEFGHIJKMLNOP", MESSAGE_SIZE);

    char recv_buf[MESSAGE_SIZE];
    struct timeval start, end;

    // Hint 1: register the "connected" client_thread's socket in the its epoll instance
    // Hint 2: use gettimeofday() and "struct timeval start, end" to record timestamp, which can be used to calculated RTT.

    /* Register the connected socket with this thread's epoll instance */
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.fd = data->socket_fd;

    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) == -1) {
        perror("client epoll_ctl");
        return NULL;
    }

    data->total_rtt = 0;
    data->total_messages = 0;
    data->request_rate = 0.0f;

    /* TODO: 
     * It sends messages to the server, waits for a response using epoll,
     * and measures the round-trip time (RTT) of this request-response.
     */
    for (int i = 0; i < num_requests; i++) {
        gettimeofday(&start, NULL);

        if (send_all(data->socket_fd, send_buf, MESSAGE_SIZE) != 0) {
            perror("client send");
            break;
        }

        /* Wait until we've read exactly MESSAGE_SIZE bytes back */
        size_t received = 0;
        while (received < MESSAGE_SIZE) {
            int nfds = epoll_wait(data->epoll_fd, events, MAX_EVENTS, -1);
            if (nfds == -1) {
                if (errno == EINTR) {
                    continue;
                }
                perror("client epoll_wait");
                goto done;
            }

            for (int j = 0; j < nfds; j++) {
                if (events[j].data.fd != data->socket_fd) {
                    continue;
                }

                if (events[j].events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                    fprintf(stderr, "client: server closed or error\n");
                    goto done;
                }

                if (events[j].events & EPOLLIN) {
                    ssize_t n = recv_some(data->socket_fd, recv_buf + received, MESSAGE_SIZE - received);
                    if (n > 0) {
                        received += (size_t)n;
                    } else if (n == 0) {
                        fprintf(stderr, "client: server closed connection\n");
                        goto done;
                    } else if (n == -2) {
                        /* would block; just epoll_wait again */
                        continue;
                    } else {
                        perror("client recv");
                        goto done;
                    }
                }
            }
        }

        gettimeofday(&end, NULL);
        data->total_rtt += tv_diff_us(&start, &end);
        data->total_messages += 1;
    }

    /* TODO: 
     * The function exits after sending and receiving a predefined number of messages (num_requests).
     * It calculates the request rate based on total messages and RTT
     */
done:
    /* Request rate based on accumulated RTT */
    total_rtt_s = (data->total_rtt > 0) ? ((double)data->total_rtt / 1000000.0) : 0.0;

    if (data->total_messages > 0 && total_rtt_s > 0.0) {
        data->request_rate = (float)((double)data->total_messages / total_rtt_s);
    } else {
        data->request_rate = 0.0f;
    }


    /* Cleanup per-thread resources */
    epoll_ctl(data->epoll_fd, EPOLL_CTL_DEL, data->socket_fd, NULL);
    close(data->socket_fd);
    close(data->epoll_fd);

    return NULL;
}

/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
 */
void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    struct sockaddr_in server_addr;

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        exit(1);
    }

    /* TODO:
     * Create sockets and epoll instances for client threads
     * and connect these sockets of client threads to the server
     */

    // Hint: use thread_data to save the created socket and epoll instance for each thread
    for (int i = 0; i < num_client_threads; i++) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            perror("client socket");
            exit(1);
        }

        /* Connect in blocking mode (simpler); then switch to nonblocking for epoll reads */
        if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
            perror("client connect");
            close(sockfd);
            exit(1);
        }

        if (set_nonblocking(sockfd) == -1) {
            perror("client set_nonblocking");
            close(sockfd);
            exit(1);
        }

        int epfd = epoll_create1(0);
        if (epfd == -1) {
            perror("client epoll_create1");
            close(sockfd);
            exit(1);
        }

        thread_data[i].socket_fd = sockfd;
        thread_data[i].epoll_fd = epfd;
        thread_data[i].total_rtt = 0;
        thread_data[i].total_messages = 0;
        thread_data[i].request_rate = 0.0f;
    }

    // You will pass the thread_data to pthread_create() as below
    for (int i = 0; i < num_client_threads; i++) {
        if (pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]) != 0) {
            perror("pthread_create");
            exit(1);
        }
    }

    /* TODO:
     * Wait for client threads to complete and aggregate metrics of all client threads
     */
    long long total_rtt = 0;
    long total_messages = 0;
    double total_request_rate = 0.0;

    for (int i = 0; i < num_client_threads; i++) {
        pthread_join(threads[i], NULL);

        total_rtt += thread_data[i].total_rtt;
        total_messages += thread_data[i].total_messages;
        total_request_rate += thread_data[i].request_rate;

        if (thread_data[i].total_messages > 0) {
            long long avg_rtt_thread = thread_data[i].total_rtt / thread_data[i].total_messages;
            printf("Thread %d: avg RTT = %lld us, request rate = %.2f messages/s, messages = %ld\n",
                   i, avg_rtt_thread, thread_data[i].request_rate, thread_data[i].total_messages);
        } else {
            printf("Thread %d: no completed messages\n", i);
        }
    }

    if (total_messages > 0) {
        printf("Average RTT: %lld us\n", total_rtt / total_messages);
    } else {
        printf("Average RTT: N/A (no messages)\n");
    }
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
}

/* ------------------------- Server ------------------------- */

/* epoll item types so we can store pointers in event.data.ptr */
typedef enum {
    ITEM_LISTENER = 1,
    ITEM_CONN = 2
} item_kind_t;

typedef struct {
    item_kind_t kind;
    int fd;
} ep_item_t;

typedef struct {
    ep_item_t base;                  /* kind + fd */
    int has_pending;                 /* 1 if we have bytes waiting to send */
    size_t out_len;                  /* total bytes to send (<= MESSAGE_SIZE) */
    size_t out_sent;                 /* bytes already sent */
    char out_buf[MESSAGE_SIZE];      /* pending data */
} conn_item_t;

static void close_and_free_conn(int epfd, conn_item_t *c) {
    if (!c) return;
    epoll_ctl(epfd, EPOLL_CTL_DEL, c->base.fd, NULL);
    close(c->base.fd);
    free(c);
}

static int modify_events(int epfd, conn_item_t *c, uint32_t new_events) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = new_events;
    ev.data.ptr = c;
    return epoll_ctl(epfd, EPOLL_CTL_MOD, c->base.fd, &ev);
}

void run_server() {
    /* TODO:
     * Server creates listening socket and epoll instance.
     * Server registers the listening socket to epoll
     */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        perror("server socket");
        exit(1);
    }

    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("server setsockopt");
        close(listen_fd);
        exit(1);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        close(listen_fd);
        exit(1);
    }

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("server bind");
        close(listen_fd);
        exit(1);
    }

    if (listen(listen_fd, 1024) == -1) {
        perror("server listen");
        close(listen_fd);
        exit(1);
    }

    if (set_nonblocking(listen_fd) == -1) {
        perror("server set_nonblocking listen_fd");
        close(listen_fd);
        exit(1);
    }

    int epfd = epoll_create1(0);
    if (epfd == -1) {
        perror("server epoll_create1");
        close(listen_fd);
        exit(1);
    }

    ep_item_t *listener = (ep_item_t *)calloc(1, sizeof(ep_item_t));
    if (!listener) {
        perror("calloc listener");
        close(epfd);
        close(listen_fd);
        exit(1);
    }
    listener->kind = ITEM_LISTENER;
    listener->fd = listen_fd;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.ptr = listener;

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        perror("server epoll_ctl ADD listen_fd");
        free(listener);
        close(epfd);
        close(listen_fd);
        exit(1);
    }

    printf("Server listening on %s:%d\n", server_ip, server_port);

    struct epoll_event events[MAX_EVENTS];

    /* Server's run-to-completion event loop */
    while (1) {
        /* TODO:
         * Server uses epoll to handle connection establishment with clients
         * or receive the message from clients and echo the message back
         */
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (nfds == -1) {
            if (errno == EINTR) {
                continue;
            }
            perror("server epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            ep_item_t *item = (ep_item_t *)events[i].data.ptr;
            uint32_t revents = events[i].events;

            if (!item) {
                continue;
            }

            if (item->kind == ITEM_LISTENER) {
                /* Accept all pending connections */
                while (1) {
                    struct sockaddr_in caddr;
                    socklen_t clen = sizeof(caddr);
                    int cfd = accept(listen_fd, (struct sockaddr *)&caddr, &clen);
                    if (cfd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        perror("server accept");
                        break;
                    }

                    if (set_nonblocking(cfd) == -1) {
                        perror("server set_nonblocking client");
                        close(cfd);
                        continue;
                    }

                    conn_item_t *c = (conn_item_t *)calloc(1, sizeof(conn_item_t));
                    if (!c) {
                        perror("calloc conn");
                        close(cfd);
                        continue;
                    }
                    c->base.kind = ITEM_CONN;
                    c->base.fd = cfd;
                    c->has_pending = 0;
                    c->out_len = 0;
                    c->out_sent = 0;

                    struct epoll_event cev;
                    memset(&cev, 0, sizeof(cev));
                    cev.events = EPOLLIN | EPOLLRDHUP;
                    cev.data.ptr = c;

                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev) == -1) {
                        perror("server epoll_ctl ADD client");
                        close(cfd);
                        free(c);
                        continue;
                    }
                }
                continue;
            }

            /* Connection event */
            conn_item_t *c = (conn_item_t *)item;

            if (revents & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                close_and_free_conn(epfd, c);
                continue;
            }

            /* If EPOLLOUT and pending data, flush it */
            if ((revents & EPOLLOUT) && c->has_pending) {
                while (c->out_sent < c->out_len) {
                    ssize_t n = send(c->base.fd, c->out_buf + c->out_sent, c->out_len - c->out_sent, 0);
                    if (n > 0) {
                        c->out_sent += (size_t)n;
                        continue;
                    }
                    if (n == -1 && errno == EINTR) {
                        continue;
                    }
                    if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        /* still not writable */
                        break;
                    }
                    /* hard error */
                    close_and_free_conn(epfd, c);
                    c = NULL;
                    break;
                }

                if (!c) {
                    continue;
                }

                if (c->out_sent == c->out_len) {
                    /* done sending; go back to reading */
                    c->has_pending = 0;
                    c->out_len = 0;
                    c->out_sent = 0;
                    if (modify_events(epfd, c, EPOLLIN | EPOLLRDHUP) == -1) {
                        perror("server epoll_ctl MOD back to EPOLLIN");
                        close_and_free_conn(epfd, c);
                    }
                }
            }

            if (!c) {
                continue;
            }

            /* If we already have pending output, don't read more (simple backpressure) */
            if (c->has_pending) {
                continue;
            }

            /* Read and echo */
            if (revents & EPOLLIN) {
                while (1) {
                    char buf[MESSAGE_SIZE];
                    ssize_t n = recv_some(c->base.fd, buf, MESSAGE_SIZE);
                    if (n > 0) {
                        /* Try immediate send */
                        size_t sent = 0;
                        while (sent < (size_t)n) {
                            ssize_t m = send(c->base.fd, buf + sent, (size_t)n - sent, 0);
                            if (m > 0) {
                                sent += (size_t)m;
                                continue;
                            }
                            if (m == -1 && errno == EINTR) {
                                continue;
                            }
                            if (m == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                                /* Save remaining bytes and wait for EPOLLOUT */
                                c->has_pending = 1;
                                c->out_len = (size_t)n;
                                c->out_sent = sent;
                                memcpy(c->out_buf, buf, (size_t)n);

                                if (modify_events(epfd, c, EPOLLOUT | EPOLLRDHUP) == -1) {
                                    perror("server epoll_ctl MOD to EPOLLOUT");
                                    close_and_free_conn(epfd, c);
                                }
                                break;
                            }
                            /* hard error */
                            close_and_free_conn(epfd, c);
                            c = NULL;
                            break;
                        }

                        if (!c) {
                            break;
                        }
                        if (c->has_pending) {
                            /* stop reading until we flush pending output */
                            break;
                        }
                        /* else keep reading until EAGAIN */
                        continue;
                    }

                    if (n == 0) {
                        /* peer closed */
                        close_and_free_conn(epfd, c);
                        break;
                    }

                    if (n == -2) {
                        /* would block */
                        break;
                    }

                    /* hard error */
                    close_and_free_conn(epfd, c);
                    break;
                }
            }
        }
    }

    /* Cleanup (normally unreachable in assignment runs) */
    epoll_ctl(epfd, EPOLL_CTL_DEL, listen_fd, NULL);
    close(listen_fd);
    close(epfd);
    free(listener);
}

/* ------------------------- Main ------------------------- */

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);

        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);

        run_client();
    } else {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}
