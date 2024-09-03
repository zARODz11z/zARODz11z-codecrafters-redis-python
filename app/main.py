# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, addr = server_socket.accept()  # wait for client connection
    chunks = []
    bytes_recd = 0
    with connection:
        while True:
            chunk = connection.recv(1024)
            if not chunk:
                print("No more data")
                print(chunk)
                break
            print(chunk)
            
            # Echo back the received data
            #connection.sendall(data)
            connection.send(b"+PONG\r\n")
if __name__ == "__main__":
    main()
