import asyncio
import re

from loguru import logger
from typing import Dict
from tcp import TcpServer
from pathlib import Path
from string import printable
HOST = "0.0.0.0"
PORT = 40000


def write(writer: asyncio.StreamWriter, message):
    logger.info(f"OUT: {message}")
    if isinstance(message, bytes):
        writer.write(message)
    else:
        writer.write(message.encode())


class VoraciousCodeStorage(TcpServer):
    def __init__(self):
        super().__init__()
        self.validate_name_re = re.compile(r'^[a-zA-Z0-9._-]+$')
        self.filesystem = dict()
        self.revision_re = re.compile(r'^r(?P<rev>\d+)$')

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                write(writer, b"READY\n")
                data = await reader.readline()
                if data.upper().startswith(b"PUT"):
                    await self.put(reader, writer, data)
                elif data.upper().startswith(b"GET"):
                    await self.get(writer, data)
                elif data.upper().startswith(b"LIST"):
                    await self.list(writer, data)
                elif data.upper().startswith(b"HELP"):
                    self.help(writer, data)
                else:
                    write(writer, f"ERR illegal method: {data.split()[0]}\n".encode())
                    if data:
                        logger.error(data)
            writer.close()
            logger.info(f"{reader} closed")
        except ConnectionResetError:
            logger.error('Connection was reset')
            writer.close()
        except ConnectionAbortedError:
            logger.error('Connection was aborted')

    @logger.catch
    async def list(self, writer: asyncio.StreamWriter, data: bytes):
        logger.info(data)
        try:
            _, folder = data.strip().split()
        except ValueError:
            write(writer, b'ERR usage: LIST dir\n')
            return
        if not folder.startswith(b'/'):
            write(writer, b'ERR illegal dir name\n')
            return
        folder = Path(folder.decode())
        if folder in self.filesystem:

            items = self.filesystem[folder].items()
            directories = [[x.name + '/', 'DIR'] for x in self.filesystem if x.parent == folder and x != folder]
            items = sorted(list(items) + directories, key=lambda x: str(x[0]))

            write(writer, f"OK {len(items)}\n".encode())
            for item_name, item_data in items:
                if item_data == 'DIR':
                    write(writer, f"{item_name} DIR\n".encode())
                else:
                    current_rev = max(item_data.keys())
                    write(writer, f"{item_name} r{current_rev}\n".encode())
        else:
            write(writer, b"OK 0\n")

    @logger.catch
    async def get(self, writer: asyncio.StreamWriter, data: bytes):
        logger.info(data)
        split = data.strip().split()
        if len(split) == 1:
            write(writer, b'ERR usage: GET file [revision]\n')
            return
        file_name = self.verify_file_name(split[1].decode())
        if not file_name:
            write(writer, b'ERR illegal file name\n')
            return
        rev = None
        if len(split) == 3:
            if r := self.revision_re.match(split[2].decode()):
                rev = int(r.group('rev'))
            else:
                write(writer, b'ERR no such file\n')
                return
        if len(split) > 3:
            write(writer, b'ERR usage: GET file [revision]\n')
            return

        if rev == 0:
            write(writer, b'ERR no such file\n')
            return
        parent = file_name.parent
        if parent in self.filesystem:
            if file_name.name in self.filesystem[parent]:
                rev = rev if rev else max(self.filesystem[parent][file_name.name].keys())
                if rev in self.filesystem[parent][file_name.name]:
                    write(writer, f"OK {len(self.filesystem[parent][file_name.name][rev])}\n".encode())
                    write(writer, self.filesystem[parent][file_name.name][rev])
                else:
                    write(writer, b'ERR no such file\n')
            else:
                write(writer, b'ERR no such file\n')
        else:
            write(writer, b'ERR no such file\n')

    def verify_file_name(self, file_name_str: str):
        if file_name_str.endswith('/'):
            return
        file_name = Path(file_name_str)
        if not all([self.validate_name_re.match(f) for f in file_name.parts[1:]]):
            return
        if "//" in file_name_str:
            return
        if not file_name_str.startswith('/'):
            return
        return file_name

    def verify_data(self, data: bytes):
        verified = list(filter(lambda x: chr(x) not in printable, data))
        logger.debug(verified)
        return not any(verified)

    @logger.catch
    async def put(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, data: bytes):
        logger.info(data)
        try:
            _, file_path, size = data.strip().split()
        except ValueError:
            write(writer, b'ERR usage: PUT file length newline data\n')
            return

        file_name = self.verify_file_name(file_path.decode())
        if not file_name:
            write(writer, b'ERR illegal file name\n')
            return

        parent = file_name.parent
        if parent not in self.filesystem:
            self.filesystem[parent] = dict()
        bytes_left = int(size)
        file_data = b''
        while bytes_left:
            file_data += await reader.read(bytes_left)
            bytes_left = int(size) - len(file_data)

        if not self.verify_data(file_data):
            write(writer, b'ERR illegal file name\n')
            return
        if file_name.name in self.filesystem[parent]:
            current_rev = max(self.filesystem[parent][file_name.name].keys())
            if self.filesystem[parent][file_name.name][current_rev] != file_data:
                self.filesystem[parent][file_name.name][current_rev + 1] = file_data

        else:
            self.filesystem[parent][file_name.name] = {
                1: file_data
            }
        logger.info(f"file put: {self.filesystem[parent][file_name.name]}")
        write(writer, f"OK r{max(self.filesystem[parent][file_name.name].keys())}\n".encode())

    def help(self, writer: asyncio.StreamWriter, data):
        write(writer, b'OK usage: HELP|GET|PUT|LIST\n')




if __name__ == "__main__":
    try:
        server = VoraciousCodeStorage()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
