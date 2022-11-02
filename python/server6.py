import struct
from asyncio import IncompleteReadError
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

    async def read_u8(self, reader: asyncio.StreamReader) -> int:
        return struct.unpack(">B", await reader.readexactly(1))[0]

    async def read_u16(self, reader: asyncio.StreamReader) -> int:
        return struct.unpack(">H", await reader.readexactly(2))[0]

    async def read_u32(self, reader: asyncio.StreamReader) -> int:
        return struct.unpack(">I", await reader.readexactly(4))[0]

    async def send_error(self, writer: asyncio.StreamWriter, message: str):
        writer.write(struct.pack(f">Bp", 0x10, message))

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                msg_type = await self.read_u8(reader)
                logger.info(f"{msg_type=}")
                if msg_type == 32:
                    logger.info('hello')
                if msg_type == 0x40:  # WantHeartbeat
                    heartbeat_time = await self.read_u32(reader)
                    logger.info(f'{heartbeat_time=}')
                    if heartbeat_time > 0:
                        asyncio.create_task(self.heartbeat(writer, heartbeat_time / 10))
                if msg_type == 0x80:  # IAmCamera
                    await self.camera(reader, writer)
                if msg_type == 0x81: # IAmDispatcher
                    for _ in range(await self.read_u8(reader)):

        except IncompleteReadError:
            logger.error('Incomplete read error')
            writer.close()

    async def camera(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        road, mile, limit = struct.unpack('>HHH', await reader.readexactly(6))
        if road not in self.roads:
            self.roads[road] = {}
        logger.info(f'Camera {road=} {mile=} {limit=}')
        while not reader.at_eof():
            msg_type = await self.read_u8(reader)
            if msg_type == 0x80:
                await reader.readexactly(6)
                await self.send_error(writer, 'Already Camera')
            if msg_type == 0x81:
                for _ in range(await self.read_u8(reader)):
                    await self.read_u16(reader)
                await self.send_error(writer, 'Already Camera')
            if msg_type == 0x20:
                plate_length = await self.read_u8(reader)
                plate = struct.unpack(f">{plate_length}s", await reader.readexactly(plate_length))[0]
                timestamp = await self.read_u32(reader)
                if plate not in self.roads[road]:
                    self.roads[road][plate] = []
                self.roads[road][plate].append(Sighting(mile, timestamp))
                if plate not in self.tickets and len(self.roads[road][plate]) > 1:
                    if sightings := self.limit_broken(self.roads[road][plate], limit):
                        self.send_ticket(writer, plate, road, sightings)

    def send_ticket(self, writer: asyncio.StreamWriter, plate, road, sightings: Tuple[Sighting, Sighting, int]):
        sighting1, sighting2, speed = sightings
        writer.write(struct.pack(f">BpHHIHIH", 0x21, plate, road, sighting1.mile, sighting1.timestamp, sighting2.mile,
                                 sighting2.timestamp, speed))

    def limit_broken(self, sightings: List[Sighting], limit: int) -> Optional[Tuple[Sighting, Sighting, int]]:
        sightings.sort(key=lambda x: x[0])
        for sighting1, sighting2 in zip(sightings, sightings[1:]):
            time = sighting2.timestamp - sighting1.timestamp
            distance = sighting2.mile - sighting1.mile
            speed = (distance / time) * 60 * 60
            if speed > limit:
                return sighting1, sighting2, int(speed)

    async def heartbeat(self, writer: asyncio.StreamWriter, heartbeat: float):
        logger.info(f'Beating: {heartbeat}')
        socket = writer.get_extra_info(name='socket')
        while True:
            await asyncio.sleep(heartbeat)
            logger.info('beat')
            writer.write(struct.pack(">B", 0x41))
            if socket._sock._closed:
                return


if __name__ == "__main__":
    try:
        server = SpeedDaemon()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
