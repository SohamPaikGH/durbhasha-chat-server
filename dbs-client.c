/*
 * dbs-client.c: Durbhasha Communications Client
 *
 * Author: Soham Paik
 * Description: This client uses the TCP protocol to connect to a running
 *              instance of the Hello World server and send messages.
 */

#include <arpa/inet.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define PORT "50000"
#define MAXDATASIZE 1024
#define NAMESIZE 32

volatile bool quit_flag = false;

void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET)
    return &(((struct sockaddr_in *) sa)->sin_addr);

  return &(((struct sockaddr_in6 *) sa)->sin6_addr);
}

void *handle_send_thread(void *arg) {
  int sockfd = (intptr_t) arg;
  char msg[MAXDATASIZE];

  while (1) {
    if (quit_flag) break;
    printf("> ");
    fflush(stdout);
    fgets(msg, sizeof(msg), stdin);
    msg[strcspn(msg, "\n")] = '\0';
    strncat(msg, "\n", sizeof(msg) - strlen(msg) - 1);

    if (!strlen(msg)) continue;
    if (!strncmp(msg, "\n", 1)) continue;

    int bytes = 0;
    if ((bytes = send(sockfd, msg, strlen(msg), 0)) < 0) {
      if (quit_flag) break;
      perror("send");
      break;
    }

    if (!strncmp(msg, "quit\n", 5)) break;

    memset(msg, 0, sizeof(msg));
  }

  return NULL;
}

void *handle_recv_thread(void *arg) {
  int sockfd = (intptr_t) arg;

  while (1) {
    char buf[MAXDATASIZE] = {0};
    int i = 0;
    char c;
    ssize_t bytes;
    while ((bytes = recv(sockfd, &c, 1, 0)) > 0) {
      if (i >= MAXDATASIZE - 1) {
        fprintf(stderr, "Response too long\n");
        buf[i] = 0;
        break;
      }
      if (c == '\n') {
        buf[i] = 0;
        break;
      }
      buf[i++] = c;
    }

    if (bytes <= 0) {
      fprintf(stdout, "\r%s\n", buf);
      fprintf(stdout, "Server disconnected.\n");
      quit_flag = true;
      break;
    }

    if (!strncmp(buf, "Goodbye!", 8)) {
      printf("\r%s\n", buf);
      fflush(stdout);
      quit_flag = true;
      break;
    }

    printf("\r%s\n> ", buf);
    fflush(stdout);

  }

  return NULL;
}

int main(int argc, char *argv[]) {
  int sockfd;
  struct addrinfo hints, *res0, *res; 
  int yes;
  char s[INET6_ADDRSTRLEN];

  if (argc != 2) {
    fprintf(stderr, "usage: dbs-client hostname\n");
    exit(1);
  }

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((yes = getaddrinfo(argv[1], PORT, &hints, &res)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(yes));
    exit(2);
  }

  for (res0 = res; res0 != NULL; res0 = res0->ai_next) {
    if ((sockfd = socket(res0->ai_family, res0->ai_socktype, res0->ai_protocol)) == -1) {
      continue;
    }

    if (connect(sockfd, res0->ai_addr, res0->ai_addrlen) == -1) {
      close(sockfd);
      continue;
    }

    inet_ntop(res0->ai_family, get_in_addr((struct sockaddr *) res0->ai_addr), s, sizeof(s));
    fprintf(stdout, "Connecting to %s...\n", s);
 
    break;
  }

  if (!res0) {
    fprintf(stderr, "client: failed to connect\n");
    return 2;
  }

  inet_ntop(res0->ai_family, get_in_addr((struct sockaddr *) res0->ai_addr), s, sizeof(s));
  fprintf(stdout, "Connected to %s\n", s);

  freeaddrinfo(res);

  /* Clears terminal window */
  fprintf(stdout, "\e[1;1H\e[2J");

  /* Receive the prompt from the server */
  char prompt[MAXDATASIZE] = {0}, c;
  int i = 0, bytes_received;
  while ((bytes_received = recv(sockfd, &c, 1, 0)) > 0) {
    prompt[i++] = c;
    if (c == '\n') break;
  }
  if (bytes_received <= 0) {
    fprintf(stderr, "Server disconnected.\n");
    close(sockfd);
    return EXIT_FAILURE;
  }
  prompt[strcspn(prompt, "\r\n")] = 0;

  fprintf(stdout, "%s ", prompt);
  fflush(stdout);

  char name[NAMESIZE] = {0};
  fgets(name, NAMESIZE, stdin);
  name[strcspn(name, "\r\n")] = 0;

  char msg[NAMESIZE + 1] = {0};
  snprintf(msg, sizeof(msg), "%s\n", name);
  send(sockfd, msg, strlen(msg), 0);

  pthread_t send_thread, recv_thread;
  void *arg = (void *) (intptr_t) sockfd;
  if (pthread_create(&send_thread, NULL, handle_send_thread, arg)) {
    perror("pthread_create");
    exit(1);
  }
  if (pthread_create(&recv_thread, NULL, handle_recv_thread, arg)) {
    perror("pthread_create");
    exit(1);
  }

  pthread_join(send_thread, NULL);
  pthread_join(recv_thread, NULL);

  close(sockfd);

  printf("\nConnection closed!\n");

  return 0;
}
