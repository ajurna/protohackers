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
    day: int


class Plate(NamedTuple):
    plate: str
    timestamp: int
    day: int


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
        self.tickets = defaultdict(set)
        self.dispatchers = defaultdict(list)
        self.ticket_queue = asyncio.Queue()

    async def read_u8(self, conn: Conn) -> int:
        return struct.unpack(">B", await conn.reader.readexactly(1))[0]

    async def read_u16(self, conn: Conn) -> int:
        return struct.unpack(">H", await conn.reader.readexactly(2))[0]

    async def read_u32(self, conn: Conn) -> int:
        return struct.unpack(">I", await conn.reader.readexactly(4))[0]

    async def read_str(self, conn: Conn):
        return "".join([chr(await self.read_u8(conn)) for _ in range(await self.read_u8(conn))])

    async def send_error(self, conn: Conn, message: str):
        conn.writer.write(b'\x10')
        conn.writer.write(struct.pack(f">B{len(message)}s", len(message),  message.encode()))
        await conn.writer.drain()

    async def start(self):
        self.server = await asyncio.start_server(self.accept_connection, self.host, self.port)
        async with self.server:
            logger.info("Server Ready.")
            await asyncio.gather(self.server.serve_forever(), self.dispatcher())

    async def dispatcher(self):
        while True:
            ticket = await self.ticket_queue.get()
            # logger.info(f'Processing ticket: {ticket=}')
            dispatchers_to_remove = []
            ticket_sent = False
            for conn in self.dispatchers[ticket.road]:
                conn: Conn
                conn.writer.write(ticket.data())
                logger.info(f'ticket sent: {ticket=}')
                ticket_sent = True
                break
            if not ticket_sent:
                await self.ticket_queue.put(ticket)
                # logger.info(f'requeue ticket: {ticket=}')
                await asyncio.sleep(1)
            for conn in dispatchers_to_remove:
                self.dispatchers[ticket.road].remove(conn)

    async def Plate(self, conn):
        plate = await self.read_str(conn)
        timestamp = await self.read_u32(conn)
        return Plate(plate, timestamp, timestamp//86400)

    async def Ticket(self, conn):
        plate = await self.read_str(conn)
        road = await self.read_u16(conn)
        mile1 = await self.read_u16(conn)
        timestamp1 = await self.read_u32(conn)
        mile2 = await self.read_u16(conn)
        timestamp2 = await self.read_u32(conn)
        speed = await self.read_u16(conn)

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

    async def add_sighting(self, plate: Plate, camera: Camera):
        logger.info(f"Sighting: {camera=} {plate=}")
        if plate.plate not in self.roads[camera.road]:
            self.roads[camera.road][plate.plate] = []
        self.roads[camera.road][plate.plate].append(Sighting(camera.mile, plate.timestamp, plate.timestamp//86400))
        if len(self.roads[camera.road][plate.plate]) > 1:
            sightings_list = await self.limit_broken(self.roads[camera.road][plate.plate], camera.limit)
            for sightings in sightings_list:
                logger.info(f"Sighting: {camera=} {plate.plate=} {sightings=}")
                s1, s2, _ = sightings
                if plate.plate not in self.tickets[s1.day] and plate.plate not in self.tickets[s2.day]:
                    self.tickets[s1.day].add(plate.plate)
                    self.tickets[s2.day].add(plate.plate)
                    await self.send_ticket(plate, camera, sightings)

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
                    case 0x10:
                        await self.read_str(conn)
                        await self.send_error(conn, 'no errors')
                    case 0x20:
                        plate = await self.Plate(conn)
                        if camera:
                            logger.info(f'Plate: {plate=}')
                            await self.add_sighting(plate, camera)
                        else:
                            await self.send_error(conn, 'no plates')
                    case 0x21:
                        await self.Ticket(conn)
                        await self.send_error(conn, 'no tickets')
                    case 0x40:  # WantHeartbeat
                        heartbeat_time = await self.WantHeartbeat(conn)
                        if not heartbeat and heartbeat_time:
                            heartbeat = asyncio.create_task(self.heartbeat(conn, heartbeat_time / 10))
                    case 0x41:
                        await self.send_error(conn, 'no heartbeat')
                    case 0x80:
                        new_camera = await self.IAmCamera(conn)
                        if not camera and not dispatcher:
                            camera = new_camera
                            logger.info(f'Camera: {camera=}')
                            if camera.road not in self.roads:
                                self.roads[camera.road] = {}
                        else:
                            logger.info('error on camera')
                            await self.send_error(conn, 'already setup')
                    case 0x81:
                        roads = await self.IAmDispatcher(conn)
                        if not dispatcher and not camera:
                            dispatcher = roads
                            for road in roads:
                                self.dispatchers[road].append(conn)
                        else:
                            await self.send_error(conn, 'already setup')
                    case _:
                        await self.send_error(conn, 'unknown message')

        except (IncompleteReadError, ConnectionResetError):
            logger.warning('Client Disconnected')
            if heartbeat:
                heartbeat.cancel()
            writer.close()

    async def send_ticket(self, plate: Plate, camera: Camera, sightings: Tuple[Sighting, Sighting, int]):
        sighting1, sighting2, speed = sightings
        ticket = Ticket(plate.plate, camera.road, sighting1.mile, sighting1.timestamp, sighting2.mile,
                        sighting2.timestamp, speed)
        await self.ticket_queue.put(ticket)
        logger.info(f'Sending Ticket: {ticket=}')

    async def limit_broken(self, sightings: List[Sighting], limit: int) -> \
            Optional[Tuple[Sighting, Sighting, int]]:
        sightings.sort(key=lambda x: x[1])
        tickets = []
        for sighting1, sighting2 in zip(sightings, sightings[1:]):
            time = abs(sighting2.timestamp - sighting1.timestamp)
            distance = abs(sighting2.mile - sighting1.mile)
            speed = int(round((distance / time) * 60 * 60))
            # logger.info(f'limit: {plate=} {time=} {distance=} {speed=} {limit=}')
            if speed > limit:
                # logger.info(f'Speed high: {sighting1=} {sighting2=}')
                tickets.append((sighting1, sighting2, speed * 100))
        return tickets

    async def heartbeat(self, conn: Conn, heartbeat: float):
        logger.info(f'Beating: {heartbeat}')
        try:
            while not conn.reader.at_eof():
                await asyncio.sleep(heartbeat)
                # logger.info('Beat')
                conn.writer.write(b"\x41")
                await conn.writer.drain()
        except ConnectionResetError:
            return
        except asyncio.CancelledError:
            return


if __name__ == "__main__":
    try:
        server = SpeedDaemon()
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info('Quitting')
        quit()
