CC = gcc
CFLAGS = -Wall -pthread
SERVER_SRC = server/server.c server/user_manager.c
SERVER_OBJ = $(SERVER_SRC:.c=.o)
CLIENT_SRC = client/chatclient.c

.PHONY: all clean

all: chatserver chatclient

chatserver: $(SERVER_SRC)
	$(CC) $(CFLAGS) -o chatserver $(SERVER_SRC)

chatclient: $(CLIENT_SRC)
	$(CC) $(CFLAGS) -o chatclient $(CLIENT_SRC)

clean:
	rm -f chatserver chatclient server/*.o client/*.o
