CC=gcc
CFLAGS=-g -Wall -Werror -Wextra

SERVER=dbs-server
CLIENT=dbs-client

MSG?="Commit"

all: $(SERVER) $(CLIENT)

$(SERVER): dbs-server.c
	$(CC) $(CFLAGS) -o $(SERVER) dbs-server.c

$(CLIENT): dbs-client.c
	$(CC) $(CFLAGS) -o $(CLIENT) dbs-client.c

clean:
	rm -f $(SERVER) $(CLIENT)

commit:
	git add dbs-client.c dbs-server.c Makefile .gitignore
	git commit -m "$(MSG)"

.PHONY: all clean commit
