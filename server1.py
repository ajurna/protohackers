import asyncio
from loguru import logger
import json
import math
from typing import Dict

HOST = "0.0.0.0"
PORT = 40000


async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    while not r.at_eof():
        data = await r.readline()
        try:
            resp = parse_response(data)+b'\n'
            logger.debug(f"{data} -> {resp}")
            w.write(resp)
            await w.drain()
        except (json.decoder.JSONDecodeError, UnicodeDecodeError, ValueError, KeyError):
            logger.debug(f"{data}")
            w.write(b'{"error":"invalid_json"}\n')
            await w.drain()
    w.close()


def parse_response(data: bytes):
    false_resp = b'{"method":"isPrime","prime":false}'
    parsed_data: Dict = json.loads(data)
    if parsed_data['method'] != 'isPrime':
        raise ValueError
    number = parsed_data['number']
    if type(number) == float:
        return false_resp
    if type(number) != int:
        raise ValueError
    if number < 2:
        return false_resp
    return json.dumps({
        'method': 'isPrime',
        "prime": is_prime(number)
    }).encode()
    

def is_prime(n):
    if n % 2 == 0 and n > 2: 
        return False
    return all(n % i for i in range(3, int(math.sqrt(n)) + 1, 2))


async def main():
    server = await asyncio.start_server(handle, HOST, PORT)
    logger.info("Server Ready.")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())