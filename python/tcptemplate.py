from tcp import TcpServer
import asyncio
from loguru import logger


class Server(TcpServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        pass


if __name__ == "__main__":
    try:
        server = Server(port=4000)
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
