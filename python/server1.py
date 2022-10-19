import asyncio
from loguru import logger
import json
import math
from typing import Dict
from tcp import TcpServer

HOST = "0.0.0.0"
PORT = 40000


class PrimeTime(TcpServer):
    def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                data = await reader.readline()
                try:
                    resp = self.parse_response(data)
                    await self.write_message(writer, resp)
                except (json.decoder.JSONDecodeError, UnicodeDecodeError):
                    await self.write_message(writer, {"error": "invalid json"})
                except ValueError:
                    await self.write_message(writer, {"error": "invalid value in number or method"})
                except KeyError:
                    await self.write_message(writer, {"error": "missing method or number"})
            writer.close()
        except ConnectionResetError:
            logger.error('Connection was reset')
            writer.close()

    async def write_message(self, writer: asyncio.StreamWriter, message: dict):
        writer.write(json.dumps(message).encode(encoding='utf8'))
        writer.write(b'\n')
        await writer.drain()

    def parse_response(self, data: bytes) -> dict:
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
            "prime": self.is_prime(number)
        }

    def is_prime(self, n):
        if n % 2 == 0 and n > 2:
            return False
        return all(n % i for i in range(3, int(math.sqrt(n)) + 1, 2))


if __name__ == "__main__":
    server = PrimeTime()
    server.start()