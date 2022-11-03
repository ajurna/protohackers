import struct
from asyncio import IncompleteReadError
from collections import defaultdict
from typing import NamedTuple, List, Tuple, Optional

from tcp import TcpServer
import asyncio
from loguru import logger


class Conn(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class Sighting(NamedTuple):
    mile: int
    timestamp: int


class Plate(NamedTuple):
    plate: str
    timestamp: int


class Camera(NamedTuple):
    road: int
    mile: int
    limit: int


class Ticket(NamedTuple):
    plate: str
    road: int
    mile1: int
    timestamp1: int
    mile2: int
    timestamp2: int
    speed: int

    def data(self) -> bytes:
        return struct.pack(f">BB{len(self.plate)}sHHIHIH", 0x21, len(self.plate), self.plate.encode(), self.road,
                           self.mile1, self.timestamp1, self.mile2, self.timestamp2, self.speed)


class SpeedDaemon(TcpServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.roads = {}
        self.tickets = set()
        self.dispatchers = defaultdict(list)
        self.ticket_queue = asyncio.Queue()

    async def read_u8(self, conn: Conn) -> int:
        return struct.unpack(">B", await conn.reader.readexactly(1))[0]

    async def read_u16(self, conn: Conn) -> int:
        return struct.unpack(">H", await conn.reader.readexactly(2))[0]

    async def read_u32(self, conn: Conn) -> int:
        return struct.unpack(">I", await conn.reader.readexactly(4))[0]

    async def read_str(self, conn: Conn, length):
        return "".join([chr(await self.read_u8(conn)) for _ in range(length)])

    async def send_error(self, conn: Conn, message: str):
        conn.writer.write(struct.pack(f">Bp", 0x10, message))
        await conn.writer.drain()

    async def start(self):
        self.server = await asyncio.start_server(self.accept_connection, self.host, self.port)
        async with self.server:
            logger.info("Server Ready.")
            await asyncio.gather(self.server.serve_forever(), self.dispatcher())

    async def dispatcher(self):
        while True:
            ticket = await self.ticket_queue.get()
            dispatchers_to_remove = []
            for writer in self.dispatchers[ticket.road]:
                writer: asyncio.StreamWriter
                socket = writer.get_extra_info(name='socket')
                if not socket._sock._closed:
                    writer.write(ticket.data())
                    break
                else:
                    dispatchers_to_remove.append(writer)
            for writer in dispatchers_to_remove:
                self.dispatchers[ticket.road].remove(writer)

    async def Plate(self, conn):
        plate_length = await self.read_u8(conn.reader)
        plate = await self.read_str(conn, plate_length)
        timestamp = await self.read_u32(conn)
        return Plate(plate, timestamp)

    async def WantHeartbeat(self, conn: Conn):
        heartbeat_time = await self.read_u32(conn)
        return heartbeat_time if heartbeat_time > 0 else None

    async def IAmCamera(self, conn: Conn):
        road, mile, limit = struct.unpack('>HHH', await conn.reader.readexactly(6))
        return Camera(road, mile, limit)

    async def IAmDispatcher(self, conn: Conn):
        new_roads = []
        for _ in range(await self.read_u8(conn)):
            road = await self.read_u16(conn)
            new_roads.append(road)
        return new_roads

    def add_sighting(self, plate):
        pass

    async def accept_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        conn = Conn(reader, writer)
        heartbeat = None
        dispatcher = False
        camera = False
        try:
            while not conn.reader.at_eof():
                msg_type = await self.read_u8(conn)
                # logger.info(f"{msg_type=}")
                match msg_type:
                    case 0x20:
                        plate = await self.Plate(conn)
                        if camera:
                            self.add_sighting(plate)  # TODO:
                    case 0x40:  # WantHeartbeat
                        heartbeat_time = await self.WantHeartbeat(conn)
                        if not heartbeat and heartbeat_time:
                            heartbeat = asyncio.create_task(self.heartbeat(reader, writer, heartbeat_time / 10))
                    case 0x80:
                        new_camera = await self.IAmCamera(conn)
                        if not camera:
                            camera = new_camera
                            if camera.road not in self.roads:
                                self.roads[camera.road] = {}
                    case 0x81:
                        roads = await self.IAmDispatcher(conn)
                        if not dispatcher and not camera:
                            for road in roads:
                                self.dispatchers[road].append(conn)
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
                        await self.send_ticket(plate, road, sightings)
        except IncompleteReadError:
            logger.error('Incomplete read error')
            writer.close()

    async def camera(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        road, mile, limit = struct.unpack('>HHH', await reader.readexactly(6))
        if road not in self.roads:
            self.roads[road] = {}
        # logger.info(f'Camera {road=} {mile=} {limit=}')
        while not reader.at_eof():
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
                        await self.send_ticket(plate, road, sightings)
            elif msg_type == 0x40:  # WantHeartbeat
                heartbeat_time = await self.read_u32(reader)
                logger.info(f'{heartbeat_time=}')
                if heartbeat_time > 0:
                    asyncio.create_task(self.heartbeat(reader, writer, heartbeat_time / 10))
            elif msg_type == 0x80:
                await reader.readexactly(6)
                await self.send_error(writer, 'Already Camera')
            elif msg_type == 0x81:
                for _ in range(await self.read_u8(reader)):
                    await self.read_u16(reader)
                await self.send_error(writer, 'Already Camera')

    async def send_ticket(self, plate, road, sightings: Tuple[Sighting, Sighting, int]):
        sighting1, sighting2, speed = sightings
        ticket = Ticket(plate, road, sighting1.mile, sighting1.timestamp, sighting2.mile, sighting2.timestamp, speed)
        await self.ticket_queue.put(ticket)
        logger.info(f'Sending Ticket: {ticket=}')

    async def limit_broken(self, sightings: List[Sighting], limit: int, plate) -> Optional[
        Tuple[Sighting, Sighting, int]]:
        sightings.sort(key=lambda x: x[1])
        for sighting1, sighting2 in zip(sightings, sightings[1:]):
            time = abs(sighting2.timestamp - sighting1.timestamp)
            distance = abs(sighting2.mile - sighting1.mile)
            speed = (distance / time) * 60 * 60
            # logger.info(f'limit: {plate=} {time=} {distance=} {speed=} {limit=}')
            if speed > limit:
                # logger.info(f'Speed high: {sighting1=} {sighting2=}')
                return sighting1, sighting2, int(round(speed, 0) * 100)

    async def heartbeat(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, heartbeat: float):
        logger.info(f'Beating: {heartbeat}')
        try:
            while not reader.at_eof():
                await asyncio.sleep(heartbeat)
                # logger.info('Beat')
                writer.write(b"\x41")
                await writer.drain()
        except ConnectionResetError:
            return


if __name__ == "__main__":
    try:
        server = SpeedDaemon()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
