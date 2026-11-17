import socket
import time

for i in range(16):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 12345))
    print(f"Connected client {i}")
    time.sleep(0.1)
    s.sendall(f"user{i}\n".encode())
    time.sleep(0.1)
    s.sendall("/join testroom\n".encode())
    time.sleep(0.1)
