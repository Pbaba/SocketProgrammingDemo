# Makefile for Part A: Concurrent Client-Server
CC = gcc
CFLAGS = -Wall -Wextra -pthread -std=c11 -O2
TARGETS = server client

all: $(TARGETS)

server: server.c
	$(CC) $(CFLAGS) -o server server.c

client: client.c
	$(CC) $(CFLAGS) -o client client.c

clean:
	rm -f $(TARGETS)

.PHONY: all clean

