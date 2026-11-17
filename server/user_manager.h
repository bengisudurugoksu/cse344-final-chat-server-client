// user_manager.h

#ifndef USER_MANAGER_H
#define USER_MANAGER_H

#define MAX_CLIENTS 100
#define MAX_USERNAME_LEN 16
#define MAX_ROOM_NAME_LEN 32

typedef struct {
    int socket_fd;
    char username[MAX_USERNAME_LEN + 1];
    char room[MAX_ROOM_NAME_LEN + 1]; // user's current room
} Client;


extern Client clients[MAX_CLIENTS];
extern int client_count;
extern pthread_mutex_t clients_mutex;

int is_valid_username(const char *username, char *error_msg);
int is_username_taken(const char *username);
int register_username(int socket_fd, const char *username);
int join_room(int socket_fd, const char *room_name);
void broadcast_to_room(int sender_fd, const char *sender_name, const char *message);
void get_client_room(int socket_fd, char *room_buf);
int send_private_message(const char *sender, const char *recipient, const char *msg);
void unregister_username(const char *username);
int get_room_user_count(const char *room_name);

#endif
