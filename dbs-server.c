/*
 * dbs-server.c: Durbhas Communications Server
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

pthread_mutex_t mutex_lock;

typedef struct client_info {
  int fd;
  char *name;
} client_info;

int client_fds[BACKLOG];

int client_add(int);

void client_remove(int);

void *client_thread(void *);

void handle_client(int, const char *);

void broadcast(const char *, const char *);

void help();

void help() {
  fprintf(stdout, "Durbhas - TCP Communications Server\n"
          "\nUsage: dbs-server [arguments]\n"
          "\nArguments:\n\t-ts\t\tThread pool mode\n\t--thread-pool\t\tThread pool mode\n");
}

void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET)
    return &(((struct sockaddr_in *) sa)->sin_addr);

  return &(((struct sockaddr_in6 *) sa)->sin6_addr);
}

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
      printf("Client disconnected.\n");
      fflush(stdout);
      break;
    }

    fflush(stdout);
    if (!strncmp(buf, "quit", 4)) {
      send(clientfd, "Goodbye!\n", 9, 0);
      send(clientfd, "\n", 1, 0);
      printf("Client disconnected.\n");
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
  client_info *c = (client_info *) arg;
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

void *pool_thread() {
  /*
  struct pollfd *fds = (struct pollfd) malloc(sizeof(pollfd) * 100);
  int nfds = 100;

  while (1) {
    struct addrinfo hints, *res, *res0;
    struct sockaddr_storage client_addr;
    socklen_t addr_size = sizeof(client_addr);

    int sockfd = accept(sockfd, client_addr, addr_size);
    if (sockfd == -1) {
      perror("accept");
      free(fds);
      return NULL;
    }

    struct pollfd new_fd;
    new_fd.fd = sockfd;
    new_fd.events = POLLIN | POLLOUT | POLLERR;
    new_fd.revents = POLLIN;

    int poll_out = poll(fds, nfds, 10000);
    if (poll_out < 0) {
      continue;
    }
    else if (poll_out) {
      // Find the fd that sent the input and send its output
    }
  }

  free(fds);
  */
  puts("This statement is a placeholder");
  return NULL;
}

int main(int argc, char **argv) {
  memset(&client_fds, -1, sizeof(client_fds));

  int sockfd, yes = 1;
  struct addrinfo hints, *res, *res0;
  struct sockaddr_storage client_addr;
  socklen_t addr_size = sizeof(client_addr);
  char s[INET6_ADDRSTRLEN];
  
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
    if (!strcmp("--thread-pool", argv[1]) || !strcmp("-tp", argv[1])) {
      fprintf(stdout, "Server running in threading pool mode...\n");

      pthread_t threads[5];
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

      /* Steps for threads
        * 1. Create five threads
        * 2. Each thread accepts a certain number of connections
        * 3. Each thread adds each accepted fd to a list of fds
        * 4. Each thread uses I/O multiplexing to handle input from connections
        * 5. Whenever a client sends input, the server sends their input to all clients
        *    (including the original sender)
       */
      if (pthread_mutex_init(&mutex_lock, NULL)) {
        fprintf(stderr, "Could not create mutex lock.\n");
        exit(3);
      }

      for (int i = 0; i < 5; i++) {
        if (pthread_create(&threads[i], &attr, &pool_thread, NULL) != 0) {
          fprintf(stderr, "Pool thread could not be created.\n");
          continue;
        }
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
    while (1) {
      int clientfd = accept(sockfd, (struct sockaddr *) &client_addr, &addr_size);
      if (clientfd == -1) {
        perror("accept");
        continue;
      }

      inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr *) &client_addr), s, sizeof(s));
      printf("Server got connection from %s\n", s);
      fflush(stdout);

      pthread_t thread;
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

      client_info *arg = (client_info *) malloc(sizeof(client_info));
      arg->fd = clientfd;
      arg->name = strdup(s);
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
