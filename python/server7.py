from tcp import TcpServer
import asyncio
from loguru import logger
import re


class MobInTheMiddle(TcpServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await self.handle_client(reader, writer)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while not reader.at_eof():
            data = (await reader.readline()).decode("utf-8")
        await writer.drain()
        writer.close()


if __name__ == "__main__":
    try:
        server = MobInTheMiddle(port=40000)
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
