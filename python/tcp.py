import asyncio
from loguru import logger


class TcpServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 40000):
        self.server = None
        self.host = host
        self.port = port

    async def start(self):
        self.server = await asyncio.start_server(self.accept_connection, self.host, self.port)
        async with self.server:
            logger.info("Server Ready.")
            await self.server.serve_forever()

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        print(f'Connection Received: {writer.get_extra_info("peername")}')
        writer.close()
