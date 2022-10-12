import asyncio
from loguru import logger
import json
import math
from typing import Dict

HOST = "0.0.0.0"
PORT = 40000


async def write_message(writer: asyncio.StreamWriter, message: dict):
    writer.write(json.dumps(message).encode(encoding='utf8'))
    writer.write(b'\n')
    await writer.drain()


async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    try:
        while not r.at_eof():
            data = await r.readline()
            try:
                resp = parse_response(data)
                await write_message(w, resp)
            except (json.decoder.JSONDecodeError, UnicodeDecodeError):
                await write_message(w, {"error": "invalid json"})
            except ValueError:
                await write_message(w, {"error": "invalid value in number or method"})
            except KeyError:
                await write_message(w, {"error": "missing method or number"})
        w.close()
    except ConnectionResetError:
        logger.error('Connection was reset')
        w.close()


def parse_response(data: bytes) -> dict:
    false_resp = {"method": "isPrime", "prime": False}
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
    return {
        'method': 'isPrime',
        "prime": is_prime(number)
    }


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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
