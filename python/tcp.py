import asyncio
import struct
from typing import NamedTuple

from loguru import logger


class Conn(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class TcpServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 40000):
        self.server = None
        self.host = host
        self.port = port

    async def read_u8(self, conn: Conn) -> int:
        return struct.unpack(">B", await conn.reader.readexactly(1))[0]

    async def read_u16(self, conn: Conn) -> int:
        return struct.unpack(">H", await conn.reader.readexactly(2))[0]

    async def read_u32(self, conn: Conn) -> int:
        return struct.unpack(">I", await conn.reader.readexactly(4))[0]

    async def start(self):
        self.server = await asyncio.start_server(self.accept_connection, self.host, self.port)
        async with self.server:
            logger.info("Server Ready.")
            await self.server.serve_forever()

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        print(f'Connection Received: {writer.get_extra_info("peername")}')
        writer.close()
