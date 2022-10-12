import asyncio
from typing import NamedTuple

from loguru import logger

HOST = "0.0.0.0"
PORT = 40000


class ReaderWriter(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class ChatServer:

    def __init__(self, host: str, port: int):
        self.connections = {}
        self.server = None
        self.host = host
        self.port = port

    async def start(self):
        self.server = await asyncio.start_server(self.accept_connection, self.host, self.port)
        async with self.server:
            logger.info("Server Ready.")
            await self.server.serve_forever()

    def broadcast(self, message: str, user=None):
        logger.info(message)
        for username, rw in self.connections.items():
            if username != user:
                rw.writer.write((message + "\n").encode("utf-8"))

    async def prompt_username(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while True:
            writer.write("Welcome to budgetchat! What shall I call you?\n".encode("utf-8"))
            data = (await reader.readline()).decode("utf-8")
            username = data.strip()
            if not username.isalnum():
                writer.write("Sorry, that username is invalid.\n".encode("utf-8"))
                return None
            if username and username not in self.connections:
                writer.write(f'* The room contains: {", ".join(self.connections)}\n'.encode('utf-8'))
                await writer.drain()
                self.connections[username] = ReaderWriter(reader, writer)
                return username
            writer.write("Sorry, that username is taken.\n".encode("utf-8"))
            return None

    async def handle_connection(self, username: str, reader: asyncio.StreamReader):
        while not reader.at_eof():
            data = (await reader.readline()).decode("utf-8").strip()
            if len(data) == 0:
                continue
            self.broadcast(f"[{username}] {data}", user=username)
        del self.connections[username]

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        username = await self.prompt_username(reader, writer)
        if username is not None:
            self.broadcast(f"* {username} has entered the room", user=username)
            await self.handle_connection(username, reader)
            self.broadcast(f"* {username} has left the room")
        await writer.drain()
        writer.close()


async def main():
    server = ChatServer(HOST, PORT)
    await server.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
