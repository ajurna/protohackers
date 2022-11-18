import asyncio
import re
from dataclasses import dataclass
from math import ceil
from typing import Dict, NamedTuple
from datetime import datetime, timedelta
from loguru import logger

HOST = "0.0.0.0"
PORT = 54321


class Session:
    def __init__(self, addr, transport, session):
        self.addr = addr
        self.transport = transport
        self.session = session
        self.data = {}
        self.sent_counter = 0
        self.buffer = ''
        self.closed = False
        self.last_message_time = datetime.now()
        self.largest_ack = 0
        try:
            self.timeout = asyncio.create_task(self.timeout_check())
        except RuntimeError:
            pass
        self.last_message = ''
        self.messages_unacked = {}

    async def timeout_check(self):
        while datetime.now() - self.last_message_time < timedelta(seconds=60) and not self.closed:
            await asyncio.sleep(3)
            if datetime.now() - self.last_message_time > timedelta(seconds=3) and self.last_message:
                for message in self.messages_unacked.values():
                    self.send_message(message)
        self.closed = True

    def ack(self):
        message = f"/ack/{self.session}/0/"
        self.send_message(message)

    def send_closed_message(self):
        message = f"/close/{self.session}/"
        self.send_message(message)

    def check_for_line(self):
        self.last_message_time = datetime.now()
        to_remove = []
        for k, v in sorted(self.data.items()):
            if k == len(self.buffer):
                self.buffer += v
                to_remove.append(k)
            elif k < len(self.buffer):
                self.buffer = self.buffer[:k] + v + (self.buffer[k+len(v):] if k + len(v) < len(self.buffer) else '')
        for k in to_remove:
            del(self.data[k])
        self.send_message(f'/ack/{self.session}/{len(self.buffer)}/')
        searching = True
        while searching:
            try:
                ind = self.buffer.index('\n', self.sent_counter)
                reversed_data: str = self.buffer[self.sent_counter:ind]
                reversed_data = reversed_data[::-1] + '\n'
                split_size = 512
                if len(reversed_data) > split_size:
                    messages = [reversed_data[split_size*m:split_size*(m+1)] for m in range(ceil(len(reversed_data) / split_size))]
                    for i, data in enumerate(messages):
                        message_offset = self.sent_counter + (split_size * i)
                        data = data.replace('\\', '\\\\').replace('/', '\\/')
                        message = f'/data/{self.session}/{message_offset}/{data}/'
                        self.messages_unacked[message_offset+split_size] = message
                        self.send_message(message)
                else:
                    reversed_data = reversed_data.replace('\\', '\\\\').replace('/', '\\/')
                    message = f'/data/{self.session}/{self.sent_counter}/{reversed_data}/'
                    self.messages_unacked[ind] = message
                    self.send_message(message)
                self.sent_counter = ind+1
            except ValueError:
                searching = False

    def resend_data(self, ind):
        self.sent_counter = ind
        self.check_for_line()

    def send_message(self, message):
        logger.info(f"Message Sent: {message.encode()}")
        try:
            self.transport.sendto(message.encode(), self.addr)
            self.transport.sendto(message.encode(), self.addr)
        except AttributeError:
            pass


@dataclass
class Data:
    body: str
    command: str = ""
    session: str = ""
    ord: int = 0
    message: str = ""

    def __post_init__(self):
        parts = self.body.split('/', 4)
        self.command = parts[1]
        self.session = parts[2]
        if int(self.session) > 2147483648 or int(self.session) < 0:
            raise ValueError
        self.ord = int(parts[3])if parts[3].isdigit() else 0
        if self.ord > 2147483648 or self.ord < 0:
            raise ValueError
        self.message = parts[4][:-1] if len(parts) > 4 else ''
        if re.findall(r"(?<!\\)/", self.message):
            raise ValueError
        self.message = self.message.replace('\\/', '/').replace('\\\\', '\\')


class LineReversal:

    def __init__(self):
        self.sessions = {}
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        logger.info(f'Received : {data}')
        message = data.decode()

        if message.startswith('/') and message.endswith('/'):
            try:
                message_data = Data(message)
            except (IndexError, ValueError):
                logger.error('Invalid Message')
                return
            if message_data.command == "connect":
                session = self.sessions.get(message_data.session, Session(addr, self.transport, message_data.session))
                self.sessions[message_data.session] = session
                session.ack()
            elif message_data.command == "data" and message_data.session in self.sessions:
                session = self.sessions[message_data.session]
                session.data[message_data.ord] = message_data.message
                session.check_for_line()
            elif message_data.command == "close":
                if message_data.session in self.sessions:
                    session = self.sessions[message_data.session]
                    session.closed = True
                    session.send_closed_message()
            elif message_data.command == "ack" and message_data.session in self.sessions:
                session = self.sessions[message_data.session]
                session.last_message_time = datetime.now()
                if message_data.ord in session.messages_unacked:
                    del(session.messages_unacked[message_data.ord])
                if session.closed:
                    session.send_closed_message()
                elif message_data.ord <= session.largest_ack:
                    pass
                elif message_data.ord > session.sent_counter:
                    session.closed = True
                    session.send_closed_message()
                elif message_data.ord < session.sent_counter:
                    session.resend_data(message_data.ord)

    def connection_lost(self, addr):
        """ Do Nothing on Connection Lost"""
        pass


async def main():
    loop = asyncio.get_running_loop()
    logger.info("Server Ready.")
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: LineReversal(),
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
    # server = LineReversal()
    # server.datagram_received(b'/connect/1104745362/', None)
    # # server.datagram_received(b'/data/1104745362/0/hello\n/', None)
    # server.datagram_received(b'/data/1104745362/0/foo\\/bar\\/baz\nfoo\\\\bar\\\\baz\n/', None)

