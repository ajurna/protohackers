import asyncio
from loguru import logger
import json
import math
from typing import Dict
from asyncio.exceptions import IncompleteReadError
HOST = "0.0.0.0"
PORT = 40000


async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    price_data: Dict = {}
    logger.info('new session')
    try:
        while not r.at_eof():
            msg_type = await r.readexactly(1)
            int_1 = int.from_bytes(await r.readexactly(4), byteorder='big', signed=True)
            int_2 = int.from_bytes(await r.readexactly(4), byteorder='big', signed=True)
            if msg_type == b'I':
                price_data[int_1] = int_2
            if msg_type == b'Q':
                values = [v for t, v in price_data.items() if int_1 <= t <= int_2]
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
    asyncio.run(main())
