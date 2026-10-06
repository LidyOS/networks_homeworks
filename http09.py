from notes import rest
from transport import BadRequest, Buffer, MAX_LINE


async def http09(sock, store):
    reader = Buffer(sock)
    try:
        line = (await reader.readuntil(b"\r\n", MAX_LINE))[:-2]
        method, path = line.decode("ascii").split(" ")
        if method != "GET":
            raise BadRequest
        status, _, body = rest(store, method, path, {}, b"")
        await reader.loop.sock_sendall(sock, body if status == 200 else b"not found\n")
    except (BadRequest, ValueError, UnicodeDecodeError):
        pass


