"""Notes service with four TCP listeners."""

import argparse
import asyncio
import socket
from functools import partial

from http09 import http09
from http11 import http11
from http2 import http2
from notes import Notes


async def listen(port, handler, store):
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen()
    listener.setblocking(False)
    loop = asyncio.get_running_loop()

    async def client(sock):
        sock.setblocking(False)
        try:
            await handler(sock, store)
        except (EOFError, ConnectionError, asyncio.TimeoutError):
            pass
        finally:
            sock.close()

    while True:
        sock, _ = await loop.sock_accept(listener)
        asyncio.create_task(client(sock))


def main():
    parser = argparse.ArgumentParser(description="Raw TCP notes service")
    parser.add_argument("--db", default="notes.db")
    parser.add_argument("--http09-port", type=int, default=8009)
    parser.add_argument("--http11-port", type=int, default=8081)
    parser.add_argument("--http2-port", type=int, default=8082)
    parser.add_argument("--grpc-port", type=int, default=50051)
    args = parser.parse_args()
    store = Notes(args.db)

    async def run():
        await asyncio.gather(
            listen(args.http09_port, http09, store),
            listen(args.http11_port, http11, store),
            listen(args.http2_port, http2, store),
            listen(args.grpc_port, partial(http2, grpc_mode=True), store),
        )

    asyncio.run(run())


if __name__ == "__main__":
    main()
