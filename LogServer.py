import socket
import datetime
import time
import random

LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]


def generate_log_message(log_level):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M")
    return f"{log_level},{current_time}\n"


def start_server(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    print(f"Server listening on port {port}...")

    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Connection from {client_address}")

        try:
            log_messages = ""
            for _ in range(100):
                log_level = random.choice(LOG_LEVELS)
                log_messages += generate_log_message(log_level)

            client_socket.sendall(log_messages.encode())
            print("Logs sent.")
        except KeyboardInterrupt:
            print("\nServer stopped.")
            break

        client_socket.close()


if __name__ == "__main__":
    HOST = '127.0.0.1'  # Change this to your server's IP address if needed
    PORT = 8989
    start_server(HOST, PORT)