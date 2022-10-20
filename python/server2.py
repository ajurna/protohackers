import asyncio
import struct
from asyncio.exceptions import IncompleteReadError
from typing import Dict, NamedTuple
from tcp import TcpServer

from loguru import logger


class Message(NamedTuple):
    req: str
    int_1: int
    int_2: int


class MeansToAnEnd(TcpServer):
    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        price_data: Dict = {}
        logger.info('new session')
        try:
            while not reader.at_eof():
                msg = Message(*struct.unpack_from('>cii', await reader.readexactly(9)))
                if msg.req == b'I':
                    price_data[msg.int_1] = msg.int_2
                if msg.req == b'Q':
                    values = [v for t, v in price_data.items() if msg.int_1 <= t <= msg.int_2]
                    try:
                        price = sum(values)//len(values)
                    except ZeroDivisionError:
                        price = 0
                    price_out = struct.pack('>i', price)
                    logger.debug(f'{price_out=}')
                    writer.write(price_out)
            writer.close()
        except IncompleteReadError:
            logger.error('Incomplete read error')
            writer.close()


if __name__ == "__main__":
    try:
        server = MeansToAnEnd()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
