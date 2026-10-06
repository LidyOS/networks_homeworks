import asyncio

from notes import rest
from transport import BadRequest, Buffer, MAX_BODY, MAX_HEADERS, MAX_LINE


async def http11(sock, store):
    reader = Buffer(sock)
    loop = reader.loop
    while True:
        try:
            line = (await reader.readuntil(b"\r\n", MAX_LINE))[:-2].decode("ascii")
            method, path, version = line.split(" ")
            if version != "HTTP/1.1":
                raise BadRequest("HTTP/1.1 required")
            block = (await reader.readuntil(b"\r\n\r\n", MAX_HEADERS))[:-4].decode("latin-1")
            headers = {}
            for field in block.split("\r\n"):
                if ":" not in field:
                    raise BadRequest("invalid header")
                name, value = field.split(":", 1)
                key = name.lower()
                if key in ("host", "content-length", "transfer-encoding") and key in headers:
                    raise BadRequest("duplicate header")
                headers[key] = value.strip()
            if "host" not in headers:
                raise BadRequest("Host required")
            if "content-length" in headers and "transfer-encoding" in headers:
                raise BadRequest("conflicting body framing")
            if headers.get("expect", "").lower() == "100-continue":
                await loop.sock_sendall(sock, b"HTTP/1.1 100 Continue\r\n\r\n")
            if "transfer-encoding" in headers:
                if headers["transfer-encoding"].lower() != "chunked":
                    raise BadRequest("unsupported transfer encoding")
                body = bytearray()
                while True:
                    size = int((await reader.readuntil(b"\r\n", MAX_LINE))[:-2].split(b";", 1)[0], 16)
                    if size < 0 or len(body) + size > MAX_BODY:
                        raise BadRequest("body too large")
                    if size == 0:
                        trailer_size = 0
                        while True:
                            line = await reader.readuntil(b"\r\n", MAX_LINE)
                            trailer_size += len(line)
                            if trailer_size > MAX_HEADERS:
                                raise BadRequest("trailers too large")
                            if line == b"\r\n":
                                break
                        break
                    body.extend(await reader.readexactly(size))
                    if await reader.readexactly(2) != b"\r\n":
                        raise BadRequest("invalid chunk")
            else:
                size = int(headers.get("content-length", "0"))
                if size < 0 or size > MAX_BODY:
                    raise BadRequest("invalid length")
                body = await reader.readexactly(size)
            status, fields, payload = rest(store, method, path, headers, body)
            close = headers.get("connection", "").lower() == "close"
        except EOFError:
            return
        except (BadRequest, ValueError, UnicodeDecodeError, asyncio.TimeoutError):
            status, fields, payload, close = 400, {"content-type": "application/json"}, b'{"error":"bad request"}', True
        reason = {200: "OK", 201: "Created", 204: "No Content", 400: "Bad Request", 404: "Not Found",
                  405: "Method Not Allowed", 412: "Precondition Failed", 428: "Precondition Required"}[status]
        fields = dict(fields)
        if status != 204:
            fields["content-length"] = str(len(payload))
        if close:
            fields["connection"] = "close"
        head = f"HTTP/1.1 {status} {reason}\r\n" + "".join(f"{key}: {value}\r\n" for key, value in fields.items()) + "\r\n"
        await loop.sock_sendall(sock, head.encode("latin-1") + payload)
        if close:
            return
