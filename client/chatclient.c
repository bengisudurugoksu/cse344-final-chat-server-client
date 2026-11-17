#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define BUFFER_SIZE 1024

int sock;

void *receive_handler(void *arg) {
    char buffer[BUFFER_SIZE];
    while (1) {
        ssize_t len = read(sock, buffer, BUFFER_SIZE - 1);
        if (len <= 0) {
            printf("Disconnected from server.\n");
            exit(0);
        }
        buffer[len] = '\0';
        printf("%s", buffer);
        fflush(stdout);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <server_ip> <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *server_ip = argv[1];
    int port = atoi(argv[2]);

    struct sockaddr_in server_addr;
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket error");
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connect failed");
        exit(EXIT_FAILURE);
    }

    printf("Connected to the server.\n");

    pthread_t recv_thread;
    pthread_create(&recv_thread, NULL, receive_handler, NULL);

    char input[BUFFER_SIZE];
    while (fgets(input, BUFFER_SIZE, stdin)) {
        if (write(sock, input, strlen(input)) < 0) {
            perror("Write failed");
            break;
        }
    }

    close(sock);
    return 0;
}
