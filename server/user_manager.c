#include <stdio.h>  // snprintf
#include <string.h> // strlen, strcmp, strncpy
#include <unistd.h> // write
#include <pthread.h>
#include <ctype.h> // for isalnum
#include "user_manager.h"

Client clients[MAX_CLIENTS];
int client_count = 0;
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

int is_valid_username(const char *username, char *error_msg)
{
    size_t len = strlen(username);
    if (len == 0)
    {
        strcpy(error_msg, "Username cannot be empty.\n");
        return 0;
    }
    if (len > MAX_USERNAME_LEN)
    {
        strcpy(error_msg, "Username too long. Max 16 characters allowed.\n");
        return 0;
    }
    for (size_t i = 0; i < len; i++)
    {
        if (!isalnum(username[i]))
        {
            strcpy(error_msg, "Username must be alphanumeric (A-Z, a-z, 0-9).\n");
            return 0;
        }
    }
    return 1;
}

int get_room_user_count(const char *room_name)
{
    int count = 0;
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++)
    {
        if (strcmp(clients[i].room, room_name) == 0)
            count++;
    }
    pthread_mutex_unlock(&clients_mutex);
    return count;
}

int is_username_taken(const char *username)
{
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++)
    {
        if (strcmp(clients[i].username, username) == 0)
        {
            pthread_mutex_unlock(&clients_mutex);
            return 1;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    return 0;
}

int register_username(int socket_fd, const char *username)
{
    pthread_mutex_lock(&clients_mutex);
    if (client_count >= MAX_CLIENTS)
    {
        pthread_mutex_unlock(&clients_mutex);
        return 0;
    }
    strncpy(clients[client_count].username, username, MAX_USERNAME_LEN);
    clients[client_count].username[MAX_USERNAME_LEN] = '\0';
    clients[client_count].socket_fd = socket_fd;
    strncpy(clients[client_count].room, "lobby", MAX_ROOM_NAME_LEN);
    clients[client_count].room[MAX_ROOM_NAME_LEN] = '\0';
    client_count++;
    pthread_mutex_unlock(&clients_mutex);
    return 1;
}

int join_room(int socket_fd, const char *room_name)
{
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++)
    {
        if (clients[i].socket_fd == socket_fd)
        {
            strncpy(clients[i].room, room_name, MAX_ROOM_NAME_LEN);
            clients[i].room[MAX_ROOM_NAME_LEN] = '\0';
            pthread_mutex_unlock(&clients_mutex);
            return 1;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    return 0;
}

void broadcast_to_room(int sender_fd, const char *sender_name, const char *message)
{
    pthread_mutex_lock(&clients_mutex);

    char sender_room[MAX_ROOM_NAME_LEN + 1] = "";

    for (int i = 0; i < client_count; i++)
    {
        if (clients[i].socket_fd == sender_fd)
        {
            strncpy(sender_room, clients[i].room, MAX_ROOM_NAME_LEN);
            sender_room[MAX_ROOM_NAME_LEN] = '\0';
            break;
        }
    }

    for (int i = 0; i < client_count; i++)
    {
        if (clients[i].socket_fd != sender_fd &&
            strcmp(clients[i].room, sender_room) == 0)
        {

            char msg_buf[1024];
            snprintf(msg_buf, sizeof(msg_buf), "\n[%s] %s\n> ", sender_name, message);
            write(clients[i].socket_fd, msg_buf, strlen(msg_buf));
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}

void get_client_room(int socket_fd, char *room_buf)
{
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++)
    {
        if (clients[i].socket_fd == socket_fd)
        {
            strncpy(room_buf, clients[i].room, MAX_ROOM_NAME_LEN);
            room_buf[MAX_ROOM_NAME_LEN] = '\0';
            break;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

int send_private_message(const char *sender, const char *recipient, const char *msg)
{
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++)
    {
        if (strcmp(clients[i].username, recipient) == 0)
        {
            char buffer[1024];
            snprintf(buffer, sizeof(buffer), "\n[Private from %s]: %s\n", sender, msg);
            write(clients[i].socket_fd, buffer, strlen(buffer));
            write(clients[i].socket_fd, "> ", 2); // tamam
            pthread_mutex_unlock(&clients_mutex);
            return 1; // success
        }
    }
    pthread_mutex_unlock(&clients_mutex);
    return 0; // recipient not found
}

void unregister_username(const char *username) {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++) {
        if (strcmp(clients[i].username, username) == 0) {
            clients[i] = clients[client_count - 1];
            client_count--;
            break;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

