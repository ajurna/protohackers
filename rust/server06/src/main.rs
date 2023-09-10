extern crate core;

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind::WouldBlock;
use std::sync::Arc;
use std::sync::{Arc};
use tokio::time::sleep;
use std::time::Duration;

use queues::{IsQueue, Queue};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

type ConnectionHandler = Arc<Mutex<BufWriter<OwnedWriteHalf>>>;
#[derive(Clone, Debug)]
struct Sighting {
    mile: u16,
    timestamp: u32,
    day: u32
}
#[derive(Debug)]
struct Plate {
    plate: String,
    timestamp: u32
}
type Road = u16;

type RoadData = HashMap<String, Vec<Sighting>>;
type SightingData = Arc<Mutex<HashMap<Road, RoadData>>>;
type Dispatchers = Arc<Mutex<HashMap<Road, Queue<ConnectionHandler>>>>;

#[derive(Clone, Debug)]
struct Ticket {
    plate: String,
    road: Road,
    mile1: u16,
    timestamp1: u32,
    mile2: u16,
    timestamp2: u32,
    speed: u16
}

impl Ticket {
    fn to_bytes(&self) -> Vec<u8> {

        let mut out:Vec<u8> = vec![];
        out.push(0x21 as u8);
        out.push(self.plate.len() as u8);
        out.extend(self.plate.as_bytes());
        out.extend(self.road.to_be_bytes());
        out.extend(self.mile1.to_be_bytes());
        out.extend(self.timestamp1.to_be_bytes());
        out.extend(self.mile2.to_be_bytes());
        out.extend(self.timestamp2.to_be_bytes());
        out.extend(self.speed.to_be_bytes());
        return out;
    }
}
type TicketQueue = Arc<Mutex<Queue<Ticket>>>;
type TicketSent = Arc<Mutex<HashSet<(u32, String)>>>;

#[derive(Debug)]
struct Camera {
    road: Road,
    mile: u16,
    limit: u16
}
struct LimitBroke {
    sighting1: Sighting,
    sighting2: Sighting,
    speed: u16
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    let data: SightingData = Arc::new(Mutex::new(HashMap::new()));
    let dispatchers: Dispatchers = Arc::new(Mutex::new(HashMap::new()));
    let ticket_queue: TicketQueue = Arc::new(Mutex::new(Queue::new()));
    let ticket_sent: TicketSent = Arc::new(Mutex::new(HashSet::new()));

    setup_dispatcher_thread(ticket_queue.clone(), dispatchers.clone()).await;

    println!("Ready to accept connections");
    loop {
        println!("awaiting new connection");
        let (socket, addr) = listener.accept().await?;
        println!("new connection {:?}", addr.port());
        let data = data.clone();
        let dispatchers = dispatchers.clone();
        let ticket_queue = ticket_queue.clone();
        let ticket_sent = ticket_sent.clone();
        tokio::spawn(async move {
            let mut camera: Option<Camera> = None;
            let mut heart_beating = false;
            let mut is_dispatcher = false;

            let (mut reader, writer) = socket.into_split();
            let writer = BufWriter::new(writer);
            let writer: ConnectionHandler = Arc::new(Mutex::new(writer));

            // In a loop, read data from the socket and write the data back.
            loop {
                let mut buf = [0 as u8];
                let _n = match reader.try_read(&mut buf) {
                    Ok(n) if n == 0 => {
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    Ok(n) => n,
                    Err(e)  if e.kind() == WouldBlock => {
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    Err(e) => {
                        eprintln!("Error: {:?}", e);
                        return;
                    }
                };
                let msg_type = buf[0];
                match msg_type {
                    0x10 => { // Error (Server->Client)
                        write_error(&writer, "Error not accepted".to_owned()).await
                    },
                    0x20 => { // Plate (Client->Server)
                        let plate = get_plate(&mut reader).await;
                        println!("Plate: {:?}", plate);
                        match &camera {
                            Some(camera) => {
                                add_sighting(camera, plate, &data, &ticket_queue, &ticket_sent).await;
                                ()
                            },
                            None => write_error(&writer, "Plates only from camera's".to_owned()).await,
                        }
                    },
                    0x21 => { // Ticket (Server->Client)

                    },
                    0x40 => { // WantHeartbeat (Client->Server)
                        if !heart_beating {
                            setup_heartbeat(&writer, reader.read_u32().await.unwrap()).await;
                            heart_beating = true;
                        } else {
                            write_error(&writer, "Heartbeat Already Active".to_owned()).await
                        }
                        println!("Heartbeat setup")
                    },
                    0x41 => { // Heartbeat (Server->Client)
                        write_error(&writer, "Heartbeat not accepted".to_owned()).await
                    },
                    0x80 => { // IAmCamera (Client->Server)
                        let new_camera = get_camera(&mut reader).await;
                        match &camera {
                            Some(_c) => {
                                write_error(&writer, "Camera Assigned".to_owned()).await
                            },
                            None => {
                                camera=Some(new_camera);
                                println!("Camera: {:?}", camera);
                            }
                        }
                    },
                    0x81 => { // IAmDispatcher (Client->Server)
                        let dispatcher_roads = get_dispatcher(&mut reader).await;
                        if !is_dispatcher {
                            is_dispatcher = true;
                            add_dispatchers(&dispatchers, dispatcher_roads, &writer).await;
                        }
                    },
                    _ => { // Invalid Message
                        write_error(&writer, "Heartbeat not accepted".to_owned()).await;
                    }

                }
            }
        });
    }
}

async fn setup_heartbeat(writer: &ConnectionHandler, beat_length: u32){
    if beat_length == 0 {
        return;
    }
    let writer = writer.clone();
    let beat_length = Duration::from_millis((beat_length*100) as u64);
    tokio::spawn(async move {
        println!("Heartbeat: {}", beat_length.as_millis());
        loop {
            sleep(beat_length);
            {
                let mut writer = writer.lock().await;
                writer.write_u8(0x41).await.unwrap();
                match writer.flush().await {
                    Ok(_v) => (),
                    Err(_e) => return
                };
            }
        }
    });
}

async fn get_plate(reader: &mut OwnedReadHalf) -> Plate {
    let plate_length = reader.read_u8().await.unwrap();

    let mut buf:Vec<u8> = vec![0; plate_length as usize];
    reader.read_exact(&mut buf).await.unwrap();
    let plate = String::from_utf8(buf).unwrap();

    Plate { plate, timestamp: reader.read_u32().await.unwrap() }
}

async fn add_sighting(camera: &Camera, plate: Plate, data: &SightingData, ticket_queue: &TicketQueue, ticket_sent: &TicketSent) {
    let mut data = data.lock().await;
    if !data.contains_key(&camera.road) {
        data.insert(camera.road.clone(), HashMap::new());
    }
    if !data.get(&camera.road).unwrap().contains_key(&plate.plate) {
        data.get_mut(&camera.road).unwrap().insert(plate.plate.to_string(), vec![]);
    }

    let sighting = Sighting{ mile: camera.mile, timestamp: plate.timestamp, day: plate.timestamp/86400 };
    data.get_mut(&camera.road).unwrap().get_mut(&plate.plate).unwrap().push(sighting);
    if data.get(&camera.road).unwrap().get(&plate.plate).unwrap().len() > 1 {
        for limit_broken in get_limit_broken(camera, data.get_mut(&camera.road).unwrap().get_mut(&plate.plate).unwrap()).await {
            let ticket_tuple = (limit_broken.sighting1.day, plate.plate.to_owned());
            let mut ticket_sent = ticket_sent.lock().await;
            if !ticket_sent.contains(&ticket_tuple) {
                send_ticket(ticket_queue, &plate, &camera, limit_broken).await;
                ticket_sent.insert(ticket_tuple);
            }

        }
    }
}

async fn get_limit_broken(camera: &Camera, sightings: &mut Vec<Sighting>) -> Vec<LimitBroke> {
    let mut broken: Vec<LimitBroke> = vec![];

    sightings.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    for (s1, s2) in sightings.iter().zip(sightings[1..].iter()) {
        let time = (s2.timestamp as i64 - s1.timestamp as i64).abs() as f64;
        let distance = (s1.mile as i64 - s2.mile as i64).abs() as f64;
        let speed = ((distance / time) * 60.0 * 60.0).round() as u64;
        if speed > camera.limit as u64 {
            broken.push(LimitBroke{sighting1: s1.clone(), sighting2: s2.clone(), speed: (speed*100) as u16})
        }
    }
    broken
}

async fn get_camera(reader: &mut OwnedReadHalf) -> Camera{
    let road = reader.read_u16().await.unwrap();
    let mile = reader.read_u16().await.unwrap();
    let limit = reader.read_u16().await.unwrap();
    return Camera{road, mile, limit}
}

async fn get_dispatcher(reader: &mut OwnedReadHalf) -> Vec<u16>{
    let road_count = reader.read_u8().await.unwrap();
    let mut buf:Vec<u16> = vec![];
    for _ in 0..road_count {
        buf.push(reader.read_u16().await.unwrap())
    }
    println!("Dispatcher roads: {:?}", buf);
    buf
}

async fn add_dispatchers(dispatchers: &Dispatchers, roads: Vec<Road>, writer: &ConnectionHandler){
    let mut dispatchers = dispatchers.lock().await;
    for road in roads {
        let writer = writer.clone();
        if !dispatchers.contains_key(&road) {
            dispatchers.insert(road, Queue::new());
        }
        dispatchers.get_mut(&road).unwrap().add(writer).unwrap();
    }
}

async fn setup_dispatcher_thread(ticket_queue: TicketQueue, dispatchers: Dispatchers) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(1000)).await;
            {
                let mut ticket_queue = ticket_queue.lock().await;
                for _ in 0..ticket_queue.size() {
                    let ticket = ticket_queue.remove().unwrap();
                    let road = ticket.road as Road;
                    {
                        let mut dispatchers = dispatchers.lock().await;
                        let mut ticket_sent = false;
                        if dispatchers.contains_key(&road) {
                            while dispatchers.get(&road).unwrap().size() > 0 {
                                let writer = dispatchers.get_mut(&road).unwrap().remove().unwrap();
                                let mut writer_lock = writer.lock().await;
                                writer_lock.write_all(&*ticket.to_bytes()).await.unwrap();
                                match writer_lock.flush().await {
                                    Ok(_) => {
                                        drop(writer_lock);
                                        println!("Dispatchers sent: {:?}", ticket);
                                        dispatchers.get_mut(&road).unwrap().add(writer).unwrap();
                                        ticket_sent = true;
                                        break;
                                    }
                                    Err(_) => continue
                                }
                            }
                        }
                        if !ticket_sent {
                            ticket_queue.add(ticket).unwrap();
                        }
                    }
                }
            }
        }
    });
}

async fn send_ticket(ticket_queue: &TicketQueue, plate: &Plate, camera: &Camera, sighting: LimitBroke){
    let ticket = Ticket{
        plate: plate.plate.to_string(),
        road: camera.road,
        mile1: sighting.sighting1.mile,
        timestamp1: sighting.sighting1.timestamp,
        mile2: sighting.sighting2.mile,
        timestamp2: sighting.sighting2.timestamp,
        speed: sighting.speed
    };
    let mut ticket_queue = ticket_queue.lock().await;
    ticket_queue.add(ticket).unwrap();
}

async fn write_error(writer: &ConnectionHandler, message: String){
    let mut writer = writer.lock().await;
    writer.write_u8(0x10).await.unwrap();
    writer.write_u8(message.len() as u8).await.unwrap();
    writer.write_all(message.as_bytes()).await.unwrap();
}


