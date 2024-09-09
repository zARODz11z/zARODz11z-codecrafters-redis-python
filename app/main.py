# Uncomment this to pass the first stage
import sys
import socket
import asyncio
import time

redis_dict = {} # Dictionary to store key-value pairs

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
            elif cmd_name == "SET":
                print("Received SET command")
                if len(command) >= 3:
                    key = command[1]
                    value = command[2]
                    expiry_milliseconds_arg = command[-1] if len(command) >= 4 else None
                    print("expiry_arg: {}",expiry_milliseconds_arg)
                    time_since_epoch = int(time.time() * 1000) # Get the current time in milliseconds
                    expiry_time = time_since_epoch + int(expiry_milliseconds_arg) if expiry_milliseconds_arg else sys.maxsize
                    print("expiry_time: {}", expiry_time)
                    print("time_since_epoch: {}", time_since_epoch)
                    if expiry_time:
                        redis_dict[key] = (value, expiry_time)                    
                    
                    writer.write(b"+OK\r\n") # Respond with OK
            elif cmd_name == "GET":
                print("Received GET command")
                if len(command) == 2:
                    key = command[1]
                    time_since_epoch = int(time.time() * 1000) # Get the current time in milliseconds
                    expiry_time = redis_dict[key][1]
                    if key in redis_dict and expiry_time > time_since_epoch:
                        value = redis_dict[key][0]
                        resp = f"${len(value)}\r\n{value}\r\n".encode('utf-8')
                        writer.write(resp) # Respond with the value 
                    else:
                        writer.write(b"$-1\r\n") # Respond with nil
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