"""Minimal asyncio FIX 4.4 initiator session for the simulators."""

import asyncio
import datetime as dt
import logging

import simplefix

log = logging.getLogger("sims.fix")


class FixSessionClosed(ConnectionError):
    pass


def utc_timestamp(offset_s: float = 0.0) -> str:
    t = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=offset_s)
    return t.strftime("%Y%m%d-%H:%M:%S.") + f"{t.microsecond // 1000:03d}"


def field(msg: simplefix.FixMessage, tag: int, default=None):
    value = msg.get(tag)
    return default if value is None else value.decode()


class FixSession:
    def __init__(self, host: str, port: int, sender: str, target: str = "GATEWAY", heartbeat: int = 30):
        self.host, self.port = host, port
        self.sender, self.target = sender, target
        self.heartbeat = heartbeat
        self._seq = 1
        self._parser = simplefix.FixParser()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._hb_task: asyncio.Task | None = None
        self._last_sent = 0.0

    async def connect(self, timeout: float = 5.0) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), timeout)
        self.send("A", [(98, 0), (108, self.heartbeat), (141, "Y")])
        reply = await asyncio.wait_for(self._next_raw(), timeout)
        if reply is None or reply.get(35) != b"A":
            raise FixSessionClosed(f"{self.sender}: logon rejected")
        self._hb_task = asyncio.create_task(self._heartbeats())

    def send(self, msg_type: str, fields) -> None:
        m = simplefix.FixMessage()
        m.append_pair(8, "FIX.4.4", header=True)
        m.append_pair(35, msg_type, header=True)
        m.append_pair(49, self.sender, header=True)
        m.append_pair(56, self.target, header=True)
        m.append_pair(34, self._seq, header=True)
        m.append_utc_timestamp(52, precision=3, header=True)
        for tag, value in fields:
            m.append_pair(tag, value)
        self._seq += 1
        self._writer.write(m.encode())
        self._last_sent = asyncio.get_running_loop().time()

    async def recv(self) -> simplefix.FixMessage:
        """Next application message; session-level messages are handled internally."""
        while True:
            m = await self._next_raw()
            if m is None:
                raise FixSessionClosed(f"{self.sender}: connection closed")
            kind = m.get(35)
            if kind == b"1":
                echo = [(112, field(m, 112, ""))]
                self.send("0", echo)
            elif kind == b"5":
                self.send("5", [])
                await self.close(send_logout=False)
                raise FixSessionClosed(f"{self.sender}: logged out by gateway")
            elif kind == b"2":
                log.warning("%s: resend request ignored (reliable transport assumed)", self.sender)
            elif kind not in (b"0", b"A", b"4"):
                return m

    async def close(self, send_logout: bool = True) -> None:
        if self._hb_task:
            self._hb_task.cancel()
            self._hb_task = None
        if self._writer is None:
            return
        try:
            if send_logout:
                self.send("5", [])
                await self._writer.drain()
            self._writer.close()
            await self._writer.wait_closed()
        except (ConnectionError, OSError):
            pass
        self._writer = None

    async def _next_raw(self):
        while True:
            m = self._parser.get_message()
            if m is not None:
                return m
            data = await self._reader.read(65536)
            if not data:
                return None
            self._parser.append_buffer(data)

    async def _heartbeats(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(self.heartbeat / 2)
            if loop.time() - self._last_sent >= self.heartbeat / 2 and self._writer:
                self.send("0", [])
