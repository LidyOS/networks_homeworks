import asyncio

MAX_BODY = 1024 * 1024
MAX_LINE = 8192
MAX_HEADERS = 16384


class BadRequest(Exception):
    pass


class Buffer:
    def __init__(self, sock):
        self.sock = sock
        self.data = bytearray()
        self.loop = asyncio.get_running_loop()

    async def readexactly(self, size):
        while len(self.data) < size:
            chunk = await asyncio.wait_for(self.loop.sock_recv(self.sock, 65536), 30)
            if not chunk:
                raise EOFError
            self.data.extend(chunk)
        result = bytes(self.data[:size])
        del self.data[:size]
        return result

    async def readuntil(self, marker, limit):
        while True:
            end = self.data.find(marker)
            if end >= 0:
                if end > limit:
                    raise BadRequest("line or headers too large")
                return await self.readexactly(end + len(marker))
            if len(self.data) > limit:
                raise BadRequest("line or headers too large")
            chunk = await asyncio.wait_for(self.loop.sock_recv(self.sock, 4096), 30)
            if not chunk:
                raise EOFError
            self.data.extend(chunk)


