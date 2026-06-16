/*
 * dbs-server.c: Durbhasha Communications Server
 *
 * Author: Soham Paik
 * Purpose: This program utilizes the TCP protocol and the C POSIX Threading
 *          library to receive messages from users and broadcast them to all other
 *          users. It serves as a server for hosting a messaging server.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define PORT "50000"
#define BACKLOG 2
#define BUF_SIZE 1024
#define POOL_SIZE 5
#define NAMESIZE 32
#define TIMEOUT 1000

pthread_mutex_t mutex_lock;

typedef struct client_info_default_mode {
  char *name;
  int fd;
} client_info_default_mode;

int client_fds[BACKLOG];

/* ----------- FUNCTION PROTOTYPES ----------- */

int client_add(int);

void client_remove(int);

void *client_thread(void *);

void handle_client(int, const char *);

void broadcast(const char *, const char *);

void help();

/* ----------- POOL MODE QUEUE ----------- */

typedef struct {
  int fds[BACKLOG];
  int head, tail, count;
  pthread_mutex_t lock;
  pthread_cond_t cond;
} FdQueue;

typedef struct {
  int id;
  FdQueue queue;
} Worker;

Worker workers[POOL_SIZE];  

/* --- SHARED BROADCAST FOR POOL MODE ----- */

int pool_clients[POOL_SIZE * BACKLOG];
pthread_mutex_t pool_clients_lock = PTHREAD_MUTEX_INITIALIZER;

int pool_client_add(int fd) {
  pthread_mutex_lock(&pool_clients_lock);
  int slot = -1;
  for (int i = 0; i < POOL_SIZE * BACKLOG; i++) {
    if (pool_clients[i] == -1) {
      pool_clients[i] = fd;
      slot = i;
      break;
    }
  }
  pthread_mutex_unlock(&pool_clients_lock);

  return slot;
}

void pool_client_remove(int fd) {
  pthread_mutex_lock(&pool_clients_lock);
  for (int i = 0; i < POOL_SIZE * BACKLOG; i++) {
    if (pool_clients[i] == fd) {
      pool_clients[i] = -1;
      break;
    }
  }
  pthread_mutex_unlock(&pool_clients_lock);
}

void pool_broadcast(const char *sender_name, const char *msg) {
  char buf[BUF_SIZE];
  snprintf(buf, sizeof(buf), "[%s] says '%s'\n", sender_name, msg);

  pthread_mutex_lock(&pool_clients_lock);
  for (int i = 0; i < POOL_SIZE * BACKLOG; i++) {
    send(pool_clients[i], buf, strlen(buf), MSG_DONTWAIT);
  }
  pthread_mutex_unlock(&pool_clients_lock);
}

/* ----------- QUEUE HELPERS -------------- */

void queue_init(FdQueue *q) {
  memset(q->fds, -1, sizeof(q->fds));
  q->head = q->tail = q->count = 0;
  pthread_mutex_init(&q->lock, NULL);
  pthread_cond_init(&q->cond, NULL);
}

void queue_push(FdQueue *q, int fd) {
  pthread_mutex_lock(&q->lock);
  q->fds[q->tail] = fd;
  q->tail = (q->tail + 1) % BACKLOG;
  q->count++;
  pthread_cond_signal(&q->cond);
  pthread_mutex_unlock(&q->lock);
}

int queue_pop(FdQueue *q) {
  pthread_mutex_lock(&q->lock);
  if (q->count == 0) {
    pthread_mutex_unlock(&q->lock);
    return -1;
  }
  int fd = q->fds[q->head];
  q->head = (q->head + 1) % BACKLOG;
  q->count--;
  pthread_mutex_unlock(&q->lock);
  return fd;
}

/* ----------- HELPER FUNCTIONS ----------- */

void help() {
  fprintf(stdout, "Durbhasha - TCP Communications Server\n"
          "\nUsage: dbs-server [arguments]\n"
          "\nArguments:\n\t-ts\t\tThread pool mode\n\t--thread-pool\t\tThread pool mode\n");
}

void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET)
    return &(((struct sockaddr_in *) sa)->sin_addr);

  return &(((struct sockaddr_in6 *) sa)->sin6_addr);
}

/* ----------- DEFAULT MODE ----------- */

void broadcast(const char *sender, const char *msg) {
  char buf[BUF_SIZE];
  snprintf(buf, sizeof(buf), "[%s] says \'%s\'\n", sender, msg);

  pthread_mutex_lock(&mutex_lock);
  for (int i = 0; i < BACKLOG; i++) {
    if (client_fds[i] != -1) {
      if (send(client_fds[i], buf, strlen(buf), 0) < 0) {
        perror("send");
      }
    }
  }
  pthread_mutex_unlock(&mutex_lock);
}

void handle_client(int clientfd, const char *s) {
  char buf[BUF_SIZE];

  while (1) {
    memset(buf, 0, BUF_SIZE);
    char c;
    int i = 0;
    ssize_t bytes = 0;
    while ((bytes = recv(clientfd, &c, 1, 0)) > 0) {
      if (c == '\n') {
        buf[i] = 0;
        break;
      }
      buf[i++] = c;
    }
    if (bytes < 0) {
      perror("recv");
      break;
    }
    else if (bytes == 0) {
      fprintf(stdout, "Client %s disconnected.\n", s);
      fflush(stdout);
      break;
    }

    fflush(stdout);
    if (!strncmp(buf, "quit", 4)) {
      send(clientfd, "Goodbye!\n", 9, 0);
      send(clientfd, "\n", 1, 0);
      printf("Client %s disconnected.\n", s);
      fflush(stdout);
      break;
    }
    printf("Received from client %s: %s\n", s, buf);

    broadcast(s, buf);
  }

  return;
}

int client_add(int fd) {
  pthread_mutex_lock(&mutex_lock);
  int slot = -1;
  for (int i = 0; i < BACKLOG; i++) {
    if (client_fds[i] == -1) {
      client_fds[i] = fd;
      slot = i;
      break;
    }
  }
  pthread_mutex_unlock(&mutex_lock);
  return slot;
}

void client_remove(int idx) {
  pthread_mutex_lock(&mutex_lock);
  client_fds[idx] = -1;
  pthread_mutex_unlock(&mutex_lock);
}

void *client_thread(void *arg) {
  client_info_default_mode *c = (client_info_default_mode *) arg;
  int idx = client_add(c->fd);
  if (idx == -1) {
    fprintf(stdout, "Server full.\n");
    send(c->fd, "Server full.", 12, 0);
    shutdown(c->fd, SHUT_RDWR);
    close(c->fd);
    free(c->name);
    free(c);
    return NULL;
  }
  handle_client(c->fd, c->name);
  shutdown(c->fd, SHUT_RDWR);
  close(c->fd);
  client_remove(idx);
  free(c->name);
  free(c);
  return NULL;
}

/* ----------- POOL MODE: WORKER THREAD ----------- */

void *pool_thread(void *arg) {
  Worker *w = (Worker *) arg;

  int maxfds = BACKLOG;
  struct pollfd *fds = malloc(sizeof(struct pollfd) * maxfds);
  char **names = malloc(sizeof(char *) * maxfds);
  int nfds = 0;


  for (int i = 0; i < maxfds; i++) {
    fds[i].fd = -1;
    names[i] = NULL;
  }

  printf("Worker %d started\n", w->id);

  while (1) {
    int new_fd;

    while ((new_fd = queue_pop(&w->queue)) != -1) {
      if (nfds == maxfds) {
        maxfds *= 2;
        fds = realloc(fds, sizeof(struct pollfd) * maxfds);
        names = realloc(names, sizeof(char *) * maxfds);
      }

      fds[nfds].fd = new_fd;
      fds[nfds].events = POLLIN;
      fds[nfds].revents = 0;
      names[nfds] = NULL;
      nfds++;

      pool_client_add(new_fd);
      printf("Worker %d: added client fd=%d\n", w->id, new_fd);

      send(new_fd, "Enter your name:\n", 17, 0);
    }

    if (nfds == 0) {
      pthread_mutex_lock(&w->queue.lock);
      while (w->queue.count == 0) {
        pthread_cond_wait(&w->queue.cond, &w->queue.lock);
      }
      pthread_mutex_unlock(&w->queue.lock);
      continue;
    }

    int ready = poll(fds, nfds, TIMEOUT);
    if (ready < 0) {
      if (errno == EINTR) continue;
      perror("poll");
      continue;
    }

    for (int i = 0; i < nfds && ready; i++) {
      if (!(fds[i].revents & POLLIN)) continue;
      ready--;

      char buf[BUF_SIZE] = {0};
      ssize_t n = recv(fds[i].fd, buf, sizeof(buf) - 1, 0);

      if (n <= 0) {
        if (n == 0)
          printf("Worker %d: fd=%d disconnected\n", w->id, fds[i].fd);
        else
          perror("recv");

        pool_client_remove(fds[i].fd);
        shutdown(fds[i].fd, SHUT_RDWR);
        close(fds[i].fd);
        free(names[i]);

        fds[i] = fds[nfds - 1];
        names[i] = names[nfds - 1];
        fds[nfds - 1].fd = -1;
        names[nfds - 1] = NULL;
        nfds--;
        i--;
        continue;
      }

      buf[strcspn(buf, "\r\n")] = 0;
      if (buf[0] == 0) continue;

      if (!names[i]) {
        names[i] = strdup(buf);
        char welcome[BUF_SIZE];
        snprintf(welcome, sizeof(welcome), "Welcome, %s!\n", buf);
        send(fds[i].fd, welcome, sizeof(welcome), 0);
        printf("Worker %d: fd=%d registered as '%s'\n", w->id, fds[i].fd, buf);
        continue;
      }

      if (!strncmp(buf, "quit", 4)) {
        send(fds[i].fd, "Goodbye!\n", 9, 0);
        printf("Worker %d: [%s] quit\n", w->id, names[i]);

        pool_client_remove(fds[i].fd);
        shutdown(fds[i].fd, SHUT_RDWR);
        close(fds[i].fd);
        free(names[i]);

        fds[i] = fds[nfds - 1];
        names[i] = names[nfds - 1];
        fds[nfds - 1].fd = -1;
        names[nfds - 1] = NULL;
        nfds--;
        i--;
        continue;
      }


      printf("Worker %d: [%s] %s\n", w->id, names[i], buf);
      buf[strcspn(buf, "\r\n")] = 0;
      pool_broadcast(names[i], buf);
    }

  }

  for (int i = 0; i < nfds; i++) {
    shutdown(fds[i].fd, SHUT_RDWR);
    close(fds[i].fd);
    free(names[i]);
  }

  free(fds);
  free(names);

  return NULL;
}

/* ----------- MAIN ----------- */

int main(int argc, char **argv) {
  memset(&client_fds, -1, sizeof(client_fds));
  memset(&pool_clients, -1, sizeof(pool_clients));

  struct sigaction sa_pipe;
  memset(&sa_pipe, 0, sizeof(sa_pipe));
  sa_pipe.sa_handler = SIG_IGN;
  sigemptyset(&sa_pipe.sa_mask);
  sa_pipe.sa_flags = 0;
  if (sigaction(SIGPIPE, &sa_pipe, NULL) == -1) {
    perror("sigaction");
    exit(1);
  }

  int sockfd, yes = 1;
  struct addrinfo hints, *res, *res0;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  int status = getaddrinfo(NULL, PORT, &hints, &res);
  if (status != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(status));
    exit(2);
  }

  for (res0 = res; res0 != NULL; res0 = res->ai_next) {
    sockfd = socket(res0->ai_family, res0->ai_socktype, res0->ai_protocol);
    if (sockfd == -1) {
      perror("server: socket");
      continue;
    }

    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
      perror("server: setsockopt");
      continue;
    }

    if (bind(sockfd, res->ai_addr, res->ai_addrlen) == -1) {
      perror("server: bind");
      continue;
    }

    break;
  }

  freeaddrinfo(res);

  if (!res0) {
    fprintf(stderr, "Failed to bind to any address\n");
    exit(3);
  }

  if (listen(sockfd, BACKLOG) == -1) {
    perror("listen");
    close(sockfd);
    exit(4);
  }

  printf("Server listening on port %s\n", PORT);
  fflush(stdout);

  if (pthread_mutex_init(&mutex_lock, NULL)) {
    fprintf(stderr, "Could not create mutex lock.\n");
    exit(3);
  }

  if (argc > 1) {
    if (!strcmp("--thread-pool", argv[1]) || !strcmp("-p", argv[1])) {
      fprintf(stdout, "Server running in threading pool mode...\n");

      pthread_t threads[POOL_SIZE];
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

      if (pthread_mutex_init(&mutex_lock, NULL)) {
        fprintf(stderr, "Could not create mutex lock.\n");
        exit(3);
      }

      for (int i = 0; i < POOL_SIZE; i++) {
        workers[i].id = i;
        queue_init(&workers[i].queue);

        if (pthread_create(&threads[i], &attr, pool_thread, &workers[i]) != 0) {
          perror("pthread_create");
          exit(1);
        }
      }

      int next = 0;
      struct sockaddr_storage client_addr;
      socklen_t addr_size = sizeof(client_addr);
      char s[INET6_ADDRSTRLEN];

      while (1) {
        int clientfd = accept(sockfd, (struct sockaddr *) &client_addr, &addr_size);
        if (clientfd < 0) {
          if (errno == EINTR) continue;
          perror("accept");
        }

        inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr *) &client_addr), s, sizeof(s));
        printf("Connection from %s to worker %d\n", s, next);
        fflush(stdout);

        queue_push(&workers[next].queue, clientfd);
        next = (next + 1) % POOL_SIZE;
      }

      if (pthread_mutex_destroy(&mutex_lock) != 0) {
        fprintf(stderr, "Mutex lock could not be destroyed.");
        exit(3);
      }

    }
    else {
      for (int i = 1; i < argc; i++) {
        fprintf(stderr, "dbs-server: error: no such argument \'%s\'\n", argv[i]);
      }

      if (pthread_mutex_destroy(&mutex_lock)) {
        fprintf(stderr, "Mutex lock could not be destroyed.\n");
        exit(3);
      }
      close(sockfd);
    }
  }
  else {
    struct sockaddr_storage client_addr;
    socklen_t addr_size = sizeof(client_addr);
    char s[INET6_ADDRSTRLEN];

    while (1) {
      int clientfd = accept(sockfd, (struct sockaddr *) &client_addr, &addr_size);
      if (clientfd == -1) {
        perror("accept");
        continue;
      }

      inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr *) &client_addr), s, sizeof(s));
      printf("Server got connection from %s\n", s);
      fflush(stdout);

      /* Ask for the new user's name */
      send(clientfd, "Enter your name:\n", 17, 0);
      char name[BUF_SIZE] = {0};
      char c;
      int bytes_received, i = 0;
      while ((bytes_received = recv(clientfd, &c, 1, 0)) > 0) {
        name[i++] = c;
        if (c == '\n') break;
      }
      if (bytes_received <= 0) {
        close(clientfd);
        continue;
      }
      name[strcspn(name, "\r\n")] = 0;

      pthread_t thread;
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

      client_info_default_mode *arg = (client_info_default_mode *) malloc(sizeof(client_info_default_mode));
      arg->fd = clientfd;
      arg->name = strdup(name);
      int ret = pthread_create(&thread, &attr, &client_thread, arg);
      if (ret != 0) {
        fprintf(stderr, "Client thread could not be created.\n");
        continue;
      }

    }

    if (pthread_mutex_destroy(&mutex_lock)) {
      fprintf(stderr, "Mutex lock could not be destroyed.\n");
      exit(3);
    }
  }

  return 0;
}
