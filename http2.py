import asyncio
import struct

import hpack

from grpc_service import grpc_call
from notes import rest
from transport import BadRequest, Buffer, MAX_BODY, MAX_HEADERS

H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


def frame_bytes(kind, flags, stream, payload):
    return len(payload).to_bytes(3, "big") + bytes((kind, flags)) + stream.to_bytes(4, "big") + payload


class H2:
    def __init__(self, sock, store, grpc_mode):
        self.sock = sock
        self.reader = Buffer(sock)
        self.loop = self.reader.loop
        self.store = store
        self.grpc_mode = grpc_mode
        self.decoder = hpack.Decoder(max_header_list_size=MAX_HEADERS)
        self.encoder = hpack.Encoder()
        self.lock = asyncio.Lock()
        self.window_event = asyncio.Event()
        self.conn_window = 65535
        self.initial_window = 65535
        self.max_frame = 16384
        self.streams = {}
        self.tasks = set()

    async def frame(self, kind, flags, stream, payload=b""):
        async with self.lock:
            await self.loop.sock_sendall(self.sock, frame_bytes(kind, flags, stream, payload))

    async def headers(self, stream, fields, end=False):
        async with self.lock:
            block = self.encoder.encode(fields)
            await self.loop.sock_sendall(self.sock, frame_bytes(1, 4 | (1 if end else 0), stream, block))

    def complete(self, stream):
        task = asyncio.create_task(self.respond(stream))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def send_body(self, stream, body, trailers=None):
        offset = 0
        while offset < len(body):
            state = self.streams.get(stream)
            if state is None:
                return
            count = max(0, min(len(body) - offset, self.max_frame, self.conn_window, state["window"]))
            if count == 0:
                self.window_event.clear()
                if self.conn_window <= 0 or state["window"] <= 0:
                    await self.window_event.wait()
                continue
            self.conn_window -= count
            state["window"] -= count
            final = offset + count == len(body) and trailers is None
            await self.frame(0, 1 if final else 0, stream, body[offset:offset + count])
            offset += count
        if trailers is not None:
            await self.headers(stream, trailers, True)
        elif not body:
            await self.frame(0, 1, stream)

    async def respond(self, stream):
        state = self.streams[stream]
        headers = state["headers"]
        body = bytes(state["body"])
        if self.grpc_mode:
            if headers.get("content-type", "").split(";", 1)[0] not in ("application/grpc", "application/grpc+proto"):
                await self.headers(stream, [(":status", "415")], True)
            else:
                code, payload, message = grpc_call(self.store, headers.get(":path", ""), body)
                await self.headers(stream, [(":status", "200"), ("content-type", "application/grpc")])
                trailers = [("grpc-status", str(code))]
                if message:
                    trailers.append(("grpc-message", message.replace("%", "%25").replace(" ", "%20")))
                await self.send_body(stream, payload, trailers)
        else:
            status, fields, payload = rest(self.store, headers.get(":method", ""), headers.get(":path", ""), headers, body)
            fields["content-length"] = str(len(payload))
            await self.headers(stream, [(":status", str(status))] + list(fields.items()), not payload)
            if payload:
                await self.send_body(stream, payload)
        self.streams.pop(stream, None)

    async def read_frame(self):
        header = await self.reader.readexactly(9)
        size = int.from_bytes(header[:3], "big")
        if size > 16384:
            raise BadRequest("frame too large")
        return header[3], header[4], int.from_bytes(header[5:], "big") & 0x7fffffff, await self.reader.readexactly(size)


async def http2(sock, store, grpc_mode=False):
    h2 = H2(sock, store, grpc_mode)
    if await h2.reader.readexactly(len(H2_PREFACE)) != H2_PREFACE:
        return
    await h2.frame(4, 0, 0)
    try:
        while True:
            kind, flags, stream, payload = await h2.read_frame()
            if kind == 4:
                if stream != 0 or (flags & 1 and payload) or len(payload) % 6:
                    raise BadRequest("invalid settings")
                if not flags & 1:
                    for offset in range(0, len(payload), 6):
                        setting, value = struct.unpack("!HI", payload[offset:offset + 6])
                        if setting == 1:
                            h2.encoder.header_table_size = value
                        elif setting == 4:
                            if value > 0x7fffffff:
                                raise BadRequest("invalid window")
                            delta = value - h2.initial_window
                            h2.initial_window = value
                            for state in h2.streams.values():
                                state["window"] += delta
                            h2.window_event.set()
                        elif setting == 5:
                            if not 16384 <= value <= 16777215:
                                raise BadRequest("invalid max frame")
                            h2.max_frame = value
                    await h2.frame(4, 1, 0)
            elif kind == 6:
                if stream != 0 or len(payload) != 8:
                    raise BadRequest("invalid ping")
                if not flags & 1:
                    await h2.frame(6, 1, 0, payload)
            elif kind == 8:
                if len(payload) != 4:
                    raise BadRequest("invalid window update")
                amount = int.from_bytes(payload, "big") & 0x7fffffff
                if not amount:
                    raise BadRequest("invalid window update")
                if stream:
                    if stream in h2.streams:
                        h2.streams[stream]["window"] += amount
                else:
                    h2.conn_window += amount
                h2.window_event.set()
            elif kind == 1:
                if not stream or stream in h2.streams:
                    raise BadRequest("invalid stream")
                end_stream = bool(flags & 1)
                if flags & 8:
                    pad = payload[0]
                    payload = payload[1:len(payload) - pad]
                if flags & 32:
                    payload = payload[5:]
                block = bytearray(payload)
                while not flags & 4:
                    next_kind, flags, next_stream, payload = await h2.read_frame()
                    if next_kind != 9 or next_stream != stream:
                        raise BadRequest("invalid continuation")
                    block.extend(payload)
                fields = dict(h2.decoder.decode(bytes(block)))
                h2.streams[stream] = {"headers": fields, "body": bytearray(), "window": h2.initial_window}
                if end_stream:
                    h2.complete(stream)
            elif kind == 0:
                if stream not in h2.streams:
                    raise BadRequest("unknown stream")
                size = len(payload)
                if flags & 8:
                    pad = payload[0]
                    payload = payload[1:len(payload) - pad]
                body = h2.streams[stream]["body"]
                if len(body) + len(payload) > MAX_BODY:
                    raise BadRequest("body too large")
                body.extend(payload)
                await h2.frame(8, 0, 0, size.to_bytes(4, "big"))
                await h2.frame(8, 0, stream, size.to_bytes(4, "big"))
                if flags & 1:
                    h2.complete(stream)
            elif kind == 3:
                h2.streams.pop(stream, None)
            elif kind == 7:
                return
    except (EOFError, BadRequest, asyncio.TimeoutError, hpack.HPACKError):
        pass
    finally:
        for task in h2.tasks:
            task.cancel()
