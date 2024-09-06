# Uncomment this to pass the first stage
import socket
import asyncio

async def handle_client(reader, writer):
    """
    Asynchronously handle communincation with a single client.
    - 'reader': A StreamReader object to read data from the  client.
    - 'writer': A StreamWriter object to send data to the client.
    """
    addr = writer.get_extra_info('peername') # Get Client address
    print(f"Got connection from {addr}")

    while True:
        data = await reader.read(1024) # Asynchronously read up to 1024 bytes
        if not data:
            print("No more data, closing connection")
            break # No more data, stop the loop
        
        print(f"Received: {data}")
        
        # Parse the RESP protocol data
        command = parse_redis_command(data)
        if command:
            cmd_name = command[0].upper() # Get the command name

            if cmd_name == "PING":
                print("Received PING command")
                writer.write(b"+PONG\r\n") # Respond with PONG
            elif cmd_name == "ECHO":
                print("Received ECHO command")
                if len(command) == 2:
                    echo_arg = command[1]
                    resp = f"${len(echo_arg)}\r\n{echo_arg}\r\n".encode('utf-8')
                    writer.write(resp) # Respond with the echo argument
                else:
                    writer.write(b"-ERR wrong number of arguments for 'ECHO' command\r\n")
            else:
                writer.write(b"-ERR unknown command\r\n")
            

            await writer.drain() # Asynchronously wait until the data is sent to the client 
        else:
            writer.write(b"-ERR invalid command\r\n")
            await writer.drain() # Asynchronously wait until the data is sent to the client
        
    print(f"Closing the connection from {addr}")
    writer.close() # Close the connection
    await writer.wait_closed() # Ensure the connection is closed

def parse_redis_command(data):
    """
    Parses RESP-encoded command into a list of arguments.
    Returns a list of strings, or None if the data is malformed.
    """
    try:
        parts = data.split(b'\r\n') # Split the data into lines
        if parts[0][0:1] == b'*': # Check if the first line starts with '*'
            num_elements = int(parts[0][1:]) # Get the number of arguments
            result = []
            idx = 1
            for _ in range(num_elements):
                # Each argument starts with $ followed by the length of the argument
                if parts[idx][0:1] == b'$':
                    arg_len = int(parts[idx][1:])
                    idx += 1
                    arg = parts[idx].decode('utf-8')
                    result.append(arg)
                    idx += 1
            return result
        
    except Exception as e:
        print(f"Error parsing command: {e}")
        return None
    
async def main():
    """
    Main server function that listens for client connections.
    """
    print("Server starting...")

    # Create the server, bind it to localhost:6379, and start accepting connections
    server = await asyncio.start_server(handle_client, 'localhost', 6379)

    # Display server details
    addr = server.sockets[0].getsockname()
    print(f"Serving on {addr}")

    # Run the serer until it's stopped
    async with server:
        await server.serve_forever()
    
if __name__ == "__main__":
    # Run the asyncio event loop
    asyncio.run(main())