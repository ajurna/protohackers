import asyncio
import json
import math
import struct
from asyncio.exceptions import IncompleteReadError
from typing import Dict, NamedTuple

from loguru import logger

HOST = "0.0.0.0"
PORT = 40000


class Message(NamedTuple):
    req: str
    int_1: int
    int_2: int


async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    price_data: Dict = {}
    logger.info('new session')
    try:
        while not r.at_eof():
            msg = Message(*struct.unpack_from('>cll', await r.readexactly(9)))
            if msg.req == b'I':
                price_data[msg.int_1] = msg.int_2
            if msg.req == b'Q':
                values = [v for t, v in price_data.items() if msg.int_1 <= t <= msg.int_2]
                try:
                    price = sum(values)//len(values)
                except ZeroDivisionError:
                    price = 0
                price_out = price.to_bytes(4, byteorder='big', signed=True)
                logger.debug(f'{price_out=}')
                w.write(price_out)
        w.close()
    except IncompleteReadError:
        logger.error('Incomplete read error')
        w.close()


async def main():
    server = await asyncio.start_server(handle, HOST, PORT)
    logger.info("Server Ready.")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
