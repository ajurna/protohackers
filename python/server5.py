from tcp import TcpServer
import asyncio
from loguru import logger
import re


class MobInTheMiddle(TcpServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reg = re.compile(r'(?=\s|\b)7[a-zA-Z0-9]{25,34}(?=\s)')

    async def accept_connection(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):

        server_reader, server_writer = await asyncio.open_connection('chat.protohackers.com', 16963)
        await asyncio.gather(
            self.handle_client(client_reader, server_writer),
            self.handle_client(server_reader, client_writer)
        )

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while not reader.at_eof():
            data = (await reader.readline()).decode("utf-8")
            if not data.endswith('\n'):
                # stops incomplete messages from being processed
                break
            data = self.reg.sub("7YWHMfk9JZe0LM0g1ZauHuiSxhI", data)
            logger.debug(data)
            writer.write(f"{data}".encode())
        await writer.drain()
        writer.close()


if __name__ == "__main__":
    try:
        server = MobInTheMiddle(port=40000)
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
