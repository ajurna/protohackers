extern crate core;

use std::collections::HashMap;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::net::{TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

type ConnectionHandler = Arc<Mutex<OwnedWriteHalf>>;
type TicketsSent = Arc<Mutex<Vec<String>>>;
struct Sighting {
    plate: String,
    timestamp: i32
}
type SightingData = Arc<Mutex<HashMap<u16, Vec<Sighting>>>>;
type Dispachers = Arc<Mutex<HashMap<u16, Vec<ConnectionHandler>>>>;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    let data: SightingData = Arc::new(Mutex::new(HashMap::new()));
    let dispatchers: Dispachers = Arc::new(Mutex::new(HashMap::new()));
    println!("Ready to accept connections");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let data = data.clone();
        let dispatchers = dispatchers.clone();
        let road: u16;
        let mile: u16;
        let limit: u16;
        tokio::spawn(async move {
            let mut is_camera = false;
            let mut heart_beating = false;
            let mut is_dispatcher = false;

            let (mut reader, writer) = socket.into_split();
            let mut writer: ConnectionHandler = Arc::new(Mutex::new(writer));

            // In a loop, read data from the socket and write the data back.
            loop {
                let msg_type = match reader.read_u8().await {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Error: {:?}", e);
                        return
                    }
                };
                println!("{:?}", msg_type);

                if msg_type == 0x40 { // Heartbeat
                    let heartbeat_length = reader.read_u32().await.unwrap();
                    if heart_beating {
                        write_error(&writer, "bad heartbeat".to_owned()).await;
                        continue;
                    }
                    if heartbeat_length > 0 {
                        let writer = writer.clone();
                        tokio::spawn(async move {
                            heartbeat(writer, heartbeat_length).await;
                        });
                        heart_beating = true;
                    }
                } else if msg_type == 0x80 { //IAmCamera
                    if is_dispatcher {
                        reader.read_u16().await.unwrap();
                        reader.read_u16().await.unwrap();
                        reader.read_u16().await.unwrap();
                        write_error(&writer, "Already Camera".to_owned()).await;
                        continue
                    }
                    let road = reader.read_u16().await.unwrap();
                    let mile= reader.read_u16().await.unwrap();
                    let limit= reader.read_u16().await.unwrap();
                    is_camera = true;
                } else if msg_type == 0x81 { //IAmDispatcher
                    if is_camera {
                        for _ in 0..reader.read_u8().await.unwrap() {
                            reader.read_u16().await.unwrap();
                        }
                        continue
                    }
                    let mut dispatchers = dispatchers.lock().await;
                    for _ in 0..reader.read_u8().await.unwrap() {
                        let road = reader.read_u16().await.unwrap();
                        if dispatchers.contains_key(&road) {
                            let writer = writer.clone();
                            let mut road_dispatchers = dispatchers.get(&road).unwrap().to_vec();
                            road_dispatchers.push(writer);
                            dispatchers.insert(road, road_dispatchers);
                        }
                    }

                } else {
                    return;
                }

            }
        });
    }
}

async fn write_error(writer: &ConnectionHandler, message: String){
    let mut writer = writer.lock().await;
    writer.write_u8(0x10).await.unwrap();
    writer.write_u8(message.len() as u8).await.unwrap();
    writer.write_all(message.as_bytes()).await.unwrap();
}

async fn heartbeat(writer: ConnectionHandler, interval: u32){
    let interval = Duration::new((interval / 10) as u64, 0);
    loop {
        sleep(interval);
        {
            let mut writer = writer.lock().await;
            writer.write_u8(0x41).await.unwrap()
        }
    }
}

