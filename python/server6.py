import struct
from asyncio import IncompleteReadError
from collections import defaultdict
from typing import NamedTuple, List, Tuple, Optional

from tcp import TcpServer
import asyncio
from loguru import logger


class Sighting(NamedTuple):
    mile: int
    timestamp: int


class SpeedDaemon(TcpServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.roads = {}
        self.tickets = set()
        self.dispatchers = defaultdict(list)
        self.ticket_queue = defaultdict(list)

    async def read_u8(self, reader: asyncio.StreamReader) -> int:
        return struct.unpack(">B", await reader.readexactly(1))[0]

    async def read_u16(self, reader: asyncio.StreamReader) -> int:
        return struct.unpack(">H", await reader.readexactly(2))[0]

    async def read_u32(self, reader: asyncio.StreamReader) -> int:
        return struct.unpack(">I", await reader.readexactly(4))[0]

    async def send_error(self, writer: asyncio.StreamWriter, message: str):
        writer.write(struct.pack(f">Bp", 0x10, message))
        await writer.drain()

    async def start(self):
        self.server = await asyncio.start_server(self.accept_connection, self.host, self.port)
        async with self.server:
            logger.info("Server Ready.")
            await asyncio.gather(self.server.serve_forever(), self.dispatcher())

    async def dispatcher(self):
        while True:
            await asyncio.sleep(2)
            # logger.info(f'Dispatcher Tick {self.ticket_queue=}')
            for road, items in self.ticket_queue.items():
                for writer in self.dispatchers[road].copy():
                    await asyncio.sleep(0)
                    writer: asyncio.StreamWriter
                    socket = writer.get_extra_info(name='socket')
                    if not socket._sock._closed:
                        writer.write(b''.join(items))
                        items.clear()
                        await writer.drain()
                    else:
                        self.dispatchers[road].remove(writer)

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                await asyncio.sleep(0)
                msg_type = await self.read_u8(reader)
                # logger.info(f"{msg_type=}")
                if msg_type == 0x40:  # WantHeartbeat
                    heartbeat_time = await self.read_u32(reader)
                    logger.info(f'{heartbeat_time=}')
                    if heartbeat_time > 0:
                        asyncio.create_task(self.heartbeat(reader, writer, heartbeat_time / 10))
                elif msg_type == 0x80:  # IAmCamera
                    await self.camera(reader, writer)
                elif msg_type == 0x81:  # IAmDispatcher
                    new_roads = []
                    for _ in range(await self.read_u8(reader)):
                        road = await self.read_u16(reader)
                        new_roads.append(road)
                        self.dispatchers[road].append(writer)
                        # logger.info(f"Dispatcher: {new_roads=}")

        except IncompleteReadError:
            logger.error('Incomplete read error')
            writer.close()

    async def camera(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        road, mile, limit = struct.unpack('>HHH', await reader.readexactly(6))
        if road not in self.roads:
            self.roads[road] = {}
        # logger.info(f'Camera {road=} {mile=} {limit=}')
        while not reader.at_eof():
            await asyncio.sleep(0)
            msg_type = await self.read_u8(reader)

            if msg_type == 0x20:
                plate_length = await self.read_u8(reader)
                plate = "".join([chr(await self.read_u8(reader)) for _ in range(plate_length)])
                timestamp = await self.read_u32(reader)
                # logger.info(f"Plate: {plate=} {mile=} {timestamp=}")
                if plate not in self.roads[road]:
                    self.roads[road][plate] = []
                self.roads[road][plate].append(Sighting(mile, timestamp))
                if plate not in self.tickets and len(self.roads[road][plate]) > 1:
                    if sightings := await self.limit_broken(self.roads[road][plate], limit, plate):
                        self.tickets.add(plate)
                        self.send_ticket(plate, road, sightings)
            elif msg_type == 0x80:
                await reader.readexactly(6)
                await self.send_error(writer, 'Already Camera')
            elif msg_type == 0x81:
                for _ in range(await self.read_u8(reader)):
                    await self.read_u16(reader)
                await self.send_error(writer, 'Already Camera')

    def send_ticket(self, plate, road, sightings: Tuple[Sighting, Sighting, int]):
        sighting1, sighting2, speed = sightings
        data = struct.pack(f">BB{len(plate)}sHHIHIH", 0x21, len(plate), plate.encode(), road, sighting1.mile, sighting1.timestamp, sighting2.mile, sighting2.timestamp, speed)
        self.ticket_queue[road].append(data)
        # logger.info(f'Sending Ticket: {plate=} {road=} {sightings=}')

    async def limit_broken(self, sightings: List[Sighting], limit: int, plate) -> Optional[Tuple[Sighting, Sighting, int]]:
        sightings.sort(key=lambda x: x[1])
        for sighting1, sighting2 in zip(sightings, sightings[1:]):
            time = abs(sighting2.timestamp - sighting1.timestamp)
            distance = abs(sighting2.mile - sighting1.mile)
            speed = (distance / time) * 60 * 60
            # logger.info(f'limit: {plate=} {time=} {distance=} {speed=} {limit=}')
            if speed > limit:
                # logger.info(f'Speed high: {sighting1=} {sighting2=}')
                return sighting1, sighting2, int(round(speed, 0)*100)

    async def heartbeat(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, heartbeat: float):
        logger.info(f'Beating: {heartbeat}')
        while not reader.at_eof():
            await asyncio.sleep(heartbeat)
            writer.write(b"\x41")
            await writer.drain()


if __name__ == "__main__":
    try:
        server = SpeedDaemon()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
