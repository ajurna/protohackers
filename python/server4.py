import asyncio
from typing import Dict, NamedTuple

from loguru import logger

HOST = "0.0.0.0"
PORT = 40000


class Message(NamedTuple):
    req: str
    int_1: int
    int_2: int


class DBServer:
    
    def __init__(self):
        self.database = {}
        self.transport = None
    
    def connection_made(self, transport):
        self.transport = transport
        
    def datagram_received(self, data, addr):
        message = data.decode()
        logger.info(f'Received : {message}')
        if '=' in message:
            key, val = message.split('=', 1)
            self.database[key] = val
        elif message == 'version':
            self.transport.sendto(b"version=Ken's Key-Value Store 1.0", addr)
        else:
            value = self.database[message]
            data = f'{message}={value}'
            self.transport.sendto(data.encode(), addr)
    
    def connection_lost(self, addr):
        """ Do Nothing on Connection Lost"""
        pass


async def main():
    loop = asyncio.get_running_loop()
    logger.info("Server Ready.")
    transport, protocol = await loop.create_datagram_endpoint(
        DBServer(),
        local_addr=(HOST, PORT))
    try:
        await loop.create_future()
    finally:
        transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
