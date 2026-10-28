#! /usr/bin/env python3
import argparse
import asyncio
import sys

SERVER_NAME = "fake-ircddb"


# Answer the handful of IRC commands a gateway needs to believe it's connected, and log everything it says
async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")
    nickname = "*"
    print(f"connected {peer}", flush=True)
    while line := await reader.readline():
        text = line.decode(errors="replace").strip()
        print(f"received {text}", flush=True)
        command, _, rest = text.partition(" ")
        command = command.upper()

        # registration
        if command == "NICK":
            nickname = rest.lstrip(":")
            writer.write(f":{SERVER_NAME} 001 {nickname} :Welcome to the fake ircDDB\r\n".encode())

        # channels and keepalives
        elif command == "JOIN":
            channel = rest.split()[0]
            writer.write(f":{nickname}!{nickname}@{SERVER_NAME} JOIN {channel}\r\n:{SERVER_NAME} 366 {nickname} {channel} :End of NAMES list\r\n".encode())
        elif command == "PING":
            writer.write(f":{SERVER_NAME} PONG {SERVER_NAME} {rest}\r\n".encode())
        elif command == "QUIT":
            break
        await writer.drain()
    writer.close()
    print(f"disconnected {peer}", flush=True)


async def serve(port):
    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    print(f"listening on {port}", flush=True)
    async with server:
        await server.serve_forever()


def main() -> int:
    # Handle program arguments
    ap = argparse.ArgumentParser(description="Fake ircDDB server for gateway tests")
    ap.add_argument("--port", type=int, default=9007, help="Port to listen on")
    args = ap.parse_args()

    asyncio.run(serve(args.port))
    return 0


if __name__ == "__main__":
    sys.exit(main())
