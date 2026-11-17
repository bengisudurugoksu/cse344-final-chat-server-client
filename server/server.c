#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <semaphore.h>
#include <time.h>
#include <stdarg.h>
#include <signal.h>
#include <stdbool.h>
#define MAX_RECIPIENTS 100
#define MAX_HISTORY 100
#include "user_manager.h"
#define MAX_USERS 100
#define MAX_ROOMS_PER_USER 32
#include <ctype.h>
#define BACKLOG 20 // At least 15 clients supported
#define MAX_UPLOADS 5
#define MAX_QUEUE_SIZE 100

typedef struct {
    char recipient[32];
    char received_filenames[MAX_HISTORY][256];
    int file_count;
} RecipientHistory;

RecipientHistory history_list[MAX_RECIPIENTS];
int history_total = 0;
pthread_mutex_t history_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct
{
    char filename[256];
    char sender[32];
    char recipient[32];
    int sender_socket;
    time_t enqueue_time;
    int enqueue_position;
} UploadRequest;

typedef struct
{
    int socket_fd;
    struct sockaddr_in client_addr;
} client_info_t;

typedef struct
{
    char username[32];
    char room_names[MAX_ROOMS_PER_USER][32];
    int room_count;
} UserRoomHistory;

UserRoomHistory user_histories[MAX_USERS];
int history_count = 0;

UploadRequest upload_queue[MAX_QUEUE_SIZE];
int queue_front = 0, queue_rear = 0;

pthread_mutex_t upload_mutex = PTHREAD_MUTEX_INITIALIZER;
sem_t upload_slots;
pthread_cond_t queue_not_empty = PTHREAD_COND_INITIALIZER;

void log_event(const char *format, ...)
{
    FILE *log_file = fopen("server.log", "a");
    if (!log_file)
        return;

    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char timebuf[64];
    strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", t);

    fprintf(log_file, "%s - ", timebuf);

    va_list args;
    va_start(args, format);
    vfprintf(log_file, format, args);
    va_end(args);

    fprintf(log_file, "\n");
    fclose(log_file);
}

int find_user_history_index(const char *username)
{
    for (int i = 0; i < history_count; i++)
    {
        if (strcmp(user_histories[i].username, username) == 0)
        {
            return i;
        }
    }
    return -1;
}

int has_joined_before(const char *username, const char *room_name)
{
    int index = find_user_history_index(username);
    if (index == -1)
        return 0;

    for (int i = 0; i < user_histories[index].room_count; i++)
    {
        if (strcmp(user_histories[index].room_names[i], room_name) == 0)
        {
            return 1;
        }
    }
    return 0;
}

void save_room_for_user(const char *username, const char *room_name)
{
    int index = find_user_history_index(username);
    if (index == -1)
    {
        if (history_count >= MAX_USERS)
            return;
        index = history_count++;
        strncpy(user_histories[index].username, username, sizeof(user_histories[index].username));
        user_histories[index].room_count = 0;
    }

    if (!has_joined_before(username, room_name))
    {
        if (user_histories[index].room_count < MAX_ROOMS_PER_USER)
        {
            strncpy(user_histories[index].room_names[user_histories[index].room_count],
                    room_name, sizeof(user_histories[index].room_names[0]));
            user_histories[index].room_count++;
        }
    }
}

void handle_sigint(int sig)
{
    log_event("[SHUTDOWN] Server is shutting down due to SIGINT");

    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < client_count; i++)
    {
        write(clients[i].socket_fd, "\n[Server]: Server shutting down.\n", 33); // tamam
        close(clients[i].socket_fd);
    }
    client_count = 0;
    pthread_mutex_unlock(&clients_mutex);

    close(STDIN_FILENO); // opsiyonel, telnet output karışmasın diye

    exit(0);
}

void *handle_client(void *arg)
{
    client_info_t *info = (client_info_t *)arg;
    int client_socket = info->socket_fd;
    struct sockaddr_in client_addr = info->client_addr;
    free(info);

    // === USERNAME SECTION START ===
    char username[32];
    while (1)
    {
        write(client_socket, "Enter your username: ", 22);
        ssize_t read_len = read(client_socket, username, sizeof(username) - 1);
        if (read_len <= 0)
        {
            close(client_socket);
            return NULL;
        }
        username[read_len] = '\0';
        username[strcspn(username, "\r\n")] = 0;

        char error_msg[64];
        if (!is_valid_username(username, error_msg))
        {
            char errbuf[128];
            snprintf(errbuf, sizeof(errbuf), "\033[31m[Server]: %s\033[0m\n", error_msg);
            write(client_socket, errbuf, strlen(errbuf));
            continue;
        }

        if (is_username_taken(username))
        {
            write(client_socket, "\033[31m[ERROR]: Username already taken. Try again.\n\033[0m", 54);
            log_event("[REJECTED] Duplicate username attempted: %s", username);
            continue;
        }

        if (!register_username(client_socket, username))
        {
            write(client_socket, "\033[31m[ERROR]: Server full. Cannot register username.\n\033[0m", 58);
            close(client_socket);
            log_event("[REJECTED] User registration attempted, server full: %s", username);
            return NULL;
        }

        break; // geçerli ve boş bir username bulunduysa çık
    }

    char welcome_msg[128];
    snprintf(welcome_msg, sizeof(welcome_msg), "\033[32mWelcome %s!\n\033[0m", username);
    write(client_socket, welcome_msg, strlen(welcome_msg));
    write(client_socket, "> ", 2);
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    log_event("[LOGIN] user '%s' connected from %s", username, client_ip);

    // === USERNAME SECTION END ===

    // === MAIN COMMUNICATION LOOP START ===
    char buffer[1024];
    ssize_t bytes_read;
    printf("[CONNECT] New client connected: %s from %s\n", username, client_ip);

    while ((bytes_read = read(client_socket, buffer, sizeof(buffer) - 1)) > 0)
    {
        buffer[bytes_read] = '\0';
        buffer[strcspn(buffer, "\r\n")] = 0;

        if (strcmp(buffer, "/exit") == 0)
        {
            write(client_socket, "\033[32m[Server]: Disconnected. Goodbye!\r\n\033[0m", 44);
            log_event("[DISCONNECT] user '%s' exited manually", username);
            break;
        }

        if (strncmp(buffer, "/join ", 6) == 0)
        {
            char *room_name = buffer + 6;
            room_name[strcspn(room_name, "\r\n")] = 0;
            if (strlen(room_name) == 0 || strlen(room_name) > MAX_ROOM_NAME_LEN)
            {
                write(client_socket, "\033[31m[ERROR]: Room name must be between 1 and 32 characters.\r\n\033[0m", 67);
                write(client_socket, "> ", 2);
                log_event("[REJECTED] Invalid room name attempted: %s", room_name);
                continue;
            }

            // alphanumeric kontrolü daima yapılmalı
            int valid_room = 1;
            for (size_t i = 0; i < strlen(room_name); i++)
            {
                if (!isalnum(room_name[i]))
                {
                    valid_room = 0;
                    break;
                }
            }

            if (!valid_room)
            {
                write(client_socket, "\033[31m[ERROR]: Room name must be alphanumeric with no spaces.\r\n\033[0m", 67);
                write(client_socket, "> ", 2);
                log_event("[REJECTED] Invalid room name attempted: %s", room_name);
                continue;
            }
            else
            {
                char current_room[MAX_ROOM_NAME_LEN + 1];
                get_client_room(client_socket, current_room);

                if (strcmp(current_room, "lobby") != 0)
                {
                    write(client_socket, "\033[31m[ERROR]: Please leave your current room using /leave before joining another one.\r\n\033[0m", 92);
                    write(client_socket, "> ", 2);
                    log_event("[REJECTED] user '%s' tried to join a room while still in '%s'", username, current_room);
                    continue;
                }
                if (get_room_user_count(room_name) >= 15)
                {
                    write(client_socket, "\033[31m[ERROR]: Room is full (max 15 users).\r\n\033[0m", 49);
                    write(client_socket, "> ", 2);
                    log_event("[REJECTED] Room '%s' is full (attempted by '%s')", room_name, username);
                    continue;
                }
                join_room(client_socket, room_name);
                if (has_joined_before(username, room_name))
                {
                    log_event("[ROOM] user '%s' rejoined '%s'", username, room_name);
                }
                else
                {
                    log_event("[JOIN] user '%s' joined room '%s'", username, room_name);
                }

                save_room_for_user(username, room_name);

                char confirm[128];
                snprintf(confirm, sizeof(confirm), "\033[32m[Server]: You joined the room '%s'\n\033[0m", room_name);
                write(client_socket, confirm, strlen(confirm));
                write(client_socket, "> ", 2);
                printf("[INFO] %s joined room '%s'\n", username, room_name);
            }
            continue;
        }
        // Handle /whisper <username> <message>
        if (strncmp(buffer, "/whisper ", 9) == 0)
        {
            char *rest = buffer + 9;
            char *space = strchr(rest, ' ');
            if (!space)
            {
                write(client_socket, "[Server]: Usage: /whisper <username> <message>\n", 48);
                write(client_socket, "> ", 2); // tamam
                continue;
            }

            *space = '\0';
            char *target = rest;
            char *msg = space + 1;

            if (send_private_message(username, target, msg))
            {
                char confirm[128];
                snprintf(confirm, sizeof(confirm), "\033[32m[Server]: Private message sent to %s\n\033[0m", target);
                write(client_socket, confirm, strlen(confirm));
                write(client_socket, "> ", 2); // tamam
                printf("[WHISPER] %s -> %s: %s", username, target, msg);
            }
            else
            {
                write(client_socket, "\033[31m[ERROR]: User not found or offline.\n\033[0m", 46);
                write(client_socket, "> ", 2); // tamam
                log_event("[REJECTED] Whisper target not found: '%s' tried to message '%s'", username, target);
            }
            continue;
        }

        // Handle /sendfile <filename> <username>
        if (strncmp(buffer, "/sendfile ", 10) == 0)
        {
            char filename[256], target[32];
            if (sscanf(buffer + 10, "%255s %31s", filename, target) != 2)
            {
                const char *usage = "[Server]: Usage: /sendfile <filename> <username>\n";
                write(client_socket, usage, strlen(usage));
                write(client_socket, "> ", 2);
                continue;
            }

            FILE *fp = fopen(filename, "rb");
            if (!fp)
            {
                write(client_socket, "\033[31m[ERROR]: Failed to open file.\n\033[0m", 35);
                write(client_socket, "> ", 2);
                log_event("[REJECTED] Failed to open file '%s' from user '%s'", filename, username);
                continue;
            }
            // Allow only .txt, .pdf, .jpg, .png
            const char *allowed_extensions[] = {".txt", ".pdf", ".jpg", ".png"};
            int valid_extension = 0;

            for (int i = 0; i < 4; i++)
            {
                if (strstr(filename, allowed_extensions[i]) != NULL)
                {
                    valid_extension = 1;
                    break;
                }
            }

            if (!valid_extension)
            {
                write(client_socket, "\033[31m[ERROR]: Invalid file type. Only .txt, .pdf, .jpg, .png allowed.\n\033[0m", 75);
                write(client_socket, "> ", 2);
                log_event("[REJECTED] Invalid file type '%s' from user '%s'", filename, username);
                fclose(fp);
                continue;
            }

            fseek(fp, 0, SEEK_END);
            long filesize = ftell(fp);
            fseek(fp, 0, SEEK_SET);

            if (filesize > 3 * 1024 * 1024)
            {
                write(client_socket, "\033[31m[ERROR]: File too large (max 3MB).\n\033[0m>", 45);
                write(client_socket, "> ", 2);
                log_event("[REJECTED] File '%s' too large from user '%s' (%ld bytes)", filename, username, filesize);
                fclose(fp);
                continue;
            }

            UploadRequest req;
            strncpy(req.filename, filename, sizeof(req.filename));
            strncpy(req.sender, username, sizeof(req.sender));
            strncpy(req.recipient, target, sizeof(req.recipient));
            req.sender_socket = client_socket;
            req.enqueue_time = time(NULL);
            pthread_mutex_lock(&upload_mutex);
            int enqueue_pos = (queue_rear - queue_front + MAX_QUEUE_SIZE) % MAX_QUEUE_SIZE;
            req.enqueue_position = enqueue_pos;

            upload_queue[queue_rear] = req;
            queue_rear = (queue_rear + 1) % MAX_QUEUE_SIZE;
            pthread_cond_signal(&queue_not_empty);
            pthread_mutex_unlock(&upload_mutex);

            write(client_socket, "\033[33m[Server]: File added to upload queue. Waiting for slot...\n\033[0m", 68);
            write(client_socket, "> ", 2);

            continue;
        }

        if (strncmp(buffer, "/leave", 6) == 0)
        {
            char current_room[MAX_ROOM_NAME_LEN + 1];
            get_client_room(client_socket, current_room);

            if (strcmp(current_room, "lobby") == 0)
            {
                write(client_socket, "\033[33m[ERROR]: You are already in the lobby.\n\033[0m", 49);
                write(client_socket, "> ", 2);
                log_event("[REJECTED] User '%s' attempted to leave while already in lobby", username);
                continue;
            }

            join_room(client_socket, "lobby");

            char msg[128];
            snprintf(msg, sizeof(msg), "\033[32m[Server]: You left the room and returned to 'lobby'\n\033[0m");
            write(client_socket, msg, strlen(msg));
            write(client_socket, "> ", 2);

            log_event("[LEAVE] user '%s' left their room and returned to 'lobby'", username);
            printf("[INFO] %s left their room and returned to 'lobby'\n", username);
            continue;
        }

        if (strncmp(buffer, "/broadcast ", 11) == 0)
        {
            char *msg = buffer + 11;
            msg[strcspn(msg, "\r\n")] = 0;

            broadcast_to_room(client_socket, username, msg);

            char current_room[MAX_ROOM_NAME_LEN + 1];
            get_client_room(client_socket, current_room);

            // Client confirmation
            char confirm[128];
            snprintf(confirm, sizeof(confirm), "\033[32m[Server]: Message sent to room '%s'\n\033[0m", current_room);
            write(client_socket, confirm, strlen(confirm));
            write(client_socket, "> ", 2);

            // Server logging (command-style)
            printf("[COMMAND] %s broadcasted to '%s'\n", username, current_room);
            log_event("[BROADCAST] user '%s': %s", username, msg);

            continue;
        }
        else
        {
            // Bilinmeyen komutsa yardım mesajı göster
            const char *help_msg =
                "\033[31m[Server]: Unknown command.\n"
                "Available commands:\n"
                "  /join <room_name>      → Join or create a room\n"
                "  /leave                 → Leave current room and go to lobby\n"
                "  /broadcast <message>   → Send message to current room\n"
                "  /whisper <user> <msg>  → Send private message\n"
                "  /sendfile <file> <user>→ Send file to a user (max 1MB)\n"
                "  /exit                  → Disconnect from the server\n\033[0m";

            write(client_socket, help_msg, strlen(help_msg));
            write(client_socket, "> ", 2);
        }

        printf("[%s] %s", username, buffer);
        // Echo for now (we'll replace this with command handling later)
        write(client_socket, buffer, bytes_read);
        write(client_socket, "> ", 2);
    }

    printf("[DISCONNECT] Client %s disconnected.\n", username);
    log_event("[DISCONNECT] user '%s' disconnected", username);
    unregister_username(username);
    close(client_socket);
    return NULL;
}

void resolve_filename_conflict(const char *original, const char *recipient, char *resolved, int max_len)
{
    pthread_mutex_lock(&history_mutex);

    RecipientHistory *rec = NULL;
    for (int i = 0; i < history_total; i++) {
        if (strcmp(history_list[i].recipient, recipient) == 0) {
            rec = &history_list[i];
            break;
        }
    }

    if (!rec) {
        rec = &history_list[history_total++];
        strncpy(rec->recipient, recipient, sizeof(rec->recipient));
        rec->file_count = 0;
    }

    int version = 0;
    for (int i = 0; i < rec->file_count; i++) {
        if (strcmp(rec->received_filenames[i], original) == 0 ||
            strncmp(rec->received_filenames[i], original, strlen(original)) == 0) {
            version++;
        }
    }

    if (version == 0) {
        snprintf(resolved, max_len, "%s", original);
    } else {
        char *ext = strrchr(original, '.');
        if (ext) {
            int base_len = ext - original;
            snprintf(resolved, max_len, "%.*s_%d%s", base_len, original, version, ext);
        } else {
            snprintf(resolved, max_len, "%s_%d", original, version);
        }
    }

    if (rec->file_count < MAX_HISTORY)
        strncpy(rec->received_filenames[rec->file_count++], resolved, 256);

    pthread_mutex_unlock(&history_mutex);
}


void *upload_worker(void *arg)
{
    while (1)
    {
        // 1. Sırada iş var mı kontrol et
        pthread_mutex_lock(&upload_mutex);
        while (queue_front == queue_rear)
            pthread_cond_wait(&queue_not_empty, &upload_mutex);
        pthread_mutex_unlock(&upload_mutex);

        // 2. Slot boş mu kontrol et (çekmeden önce bak!)
        if (sem_trywait(&upload_slots) != 0)
        {
            // SLOT DOLU → sadece kuyruktakine bak (çekme)
            UploadRequest peek;
            pthread_mutex_lock(&upload_mutex);
            peek = upload_queue[queue_front]; // sadece bak
            int qsize = (queue_rear - queue_front + MAX_QUEUE_SIZE) % MAX_QUEUE_SIZE;
            pthread_mutex_unlock(&upload_mutex);

            int sem_val;
            sem_getvalue(&upload_slots, &sem_val);
            int active_uploads = MAX_UPLOADS - sem_val;
            int position_in_system = qsize + active_uploads;

            time_t now = time(NULL);
            int wait_seconds = now - peek.enqueue_time;

            log_event("[FILE-QUEUE] Upload '%s' from user '%s' was queued. Position in system: %d (uploads: %d, in queue: %d), Waited %d seconds.",
                      peek.filename, peek.sender, position_in_system, active_uploads, qsize, wait_seconds);

            // Şimdi slot açılana kadar bekle
            sem_wait(&upload_slots);

            // Slot alınca yine logla
            now = time(NULL);
            wait_seconds = now - peek.enqueue_time;
            log_event("[FILE] '%s' from user '%s' started upload after %d seconds in queue.",
                      peek.filename, peek.sender, wait_seconds);
        }
        else
        {
            // SLOT MÜSAİTSE bile, sıradakini işleyeceğiz yine sırayla
            UploadRequest peek;
            pthread_mutex_lock(&upload_mutex);
            peek = upload_queue[queue_front]; // sadece bak
            pthread_mutex_unlock(&upload_mutex);

            time_t now = time(NULL);
            int wait_seconds = now - peek.enqueue_time;
            if (wait_seconds > 0)
            {
                log_event("[FILE-QUEUE] Upload '%s' from user '%s' was queued. Queue size at start: %d, Waited %d seconds.",
                          peek.filename, peek.sender, peek.enqueue_position, wait_seconds);
                log_event("[FILE] '%s' from user '%s' started upload after %d seconds in queue.",
                          peek.filename, peek.sender, wait_seconds);
            }
            else
            {
                log_event("[FILE] '%s' from user '%s' started upload immediately.", peek.filename, peek.sender);
            }
        }

        // 3. Slot alınmışken işi çek ve işle
        pthread_mutex_lock(&upload_mutex);
        UploadRequest req = upload_queue[queue_front];
        queue_front = (queue_front + 1) % MAX_QUEUE_SIZE;
        pthread_mutex_unlock(&upload_mutex);
        // Find recipient socket
        int recipient_socket = -1;
        pthread_mutex_lock(&clients_mutex);
        for (int i = 0; i < client_count; i++)
        {
            if (strcmp(clients[i].username, req.recipient) == 0)
            {
                recipient_socket = clients[i].socket_fd;
                break;
            }
        }
        pthread_mutex_unlock(&clients_mutex);

        if (recipient_socket == -1)
        {
            write(req.sender_socket, "\033[31m[ERROR]: Recipient not found.\n\033[0m", 40);
            write(req.sender_socket, "> ", 2);
            sem_post(&upload_slots);
            log_event("[REJECTED] File upload failed: recipient '%s' not found (from: %s, file: %s)",
                      req.recipient, req.sender, req.filename);
            continue;
        }

        FILE *fp = fopen(req.filename, "rb");
        if (!fp)
        {
            write(req.sender_socket, "\033[31m[ERROR]: Failed to open file.\n\033[0m", 40);
            write(req.sender_socket, "> ", 2);
            sem_post(&upload_slots);
            log_event("[REJECTED] File upload failed: unable to open file '%s' (sender: %s)",
                      req.filename, req.sender);
            continue;
        }

        char *data = malloc(1024 * 1024); // 1MB buffer
        long bytes_read = fread(data, 1, 1024 * 1024, fp);
        fclose(fp);

        char resolved_filename[256];
        resolve_filename_conflict(req.filename, req.recipient, resolved_filename, sizeof(resolved_filename));

        if (strcmp(req.filename, resolved_filename) != 0)
        {
            log_event("[FILE] Conflict: '%s' received again → renamed '%s'", req.filename, resolved_filename);
        }

        char notify[512];
        snprintf(notify, sizeof(notify),
                 "\033[36m[File from %s]: You received file '%s' (%ld bytes). Check your local folder.\n\033[0m> ",
                 req.sender, resolved_filename, bytes_read);
        write(recipient_socket, notify, strlen(notify));
        free(data);

        char confirm[512];
        snprintf(confirm, sizeof(confirm),
                 "\033[32m\n[Server]: File '%s' sent to %s successfully\033[0m",
                 resolved_filename, req.recipient);
        write(req.sender_socket, confirm, strlen(confirm));
        write(req.sender_socket, "\n> ", 3);

        printf("[FILE] %s sent file '%s' to %s\n", req.sender, resolved_filename, req.recipient);
        log_event("[SEND FILE] '%s' sent from %s to %s (success)",
                  resolved_filename, req.sender, req.recipient);
        sem_post(&upload_slots);
    }
    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <port>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int PORT = atoi(argv[1]); 
    signal(SIGINT, handle_sigint);
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    // Create socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1)
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Set address info
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    // Bind
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // Listen
    if (listen(server_fd, BACKLOG) < 0)
    {
        perror("Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    sem_init(&upload_slots, 0, MAX_UPLOADS); // max 5 uploads
    printf("Server is listening on port %d...\n", PORT);

    for (int i = 0; i < MAX_UPLOADS; i++)
    {
        pthread_t upload_thread;
        pthread_create(&upload_thread, NULL, upload_worker, NULL);
        pthread_detach(upload_thread);
    }

    while (1)
    {
        client_info_t *info = malloc(sizeof(client_info_t));
        if (!info)
        {
            perror("Memory allocation failed");
            continue;
        }

        info->socket_fd = accept(server_fd, (struct sockaddr *)&info->client_addr, &client_addr_len);
        if (info->socket_fd < 0)
        {
            perror("Accept failed");
            free(info);
            continue;
        }

        // Create thread to handle client
        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_client, info) != 0)
        {
            perror("Thread creation failed");
            close(info->socket_fd);
            free(info);
        }
        else
        {
            pthread_detach(tid); // Automatically clean up thread resources when done
        }
    }

    close(server_fd);
    return 0;
}
