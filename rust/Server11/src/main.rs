use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const AUTHORITY_SERVER: &str = "pestcontrol.protohackers.com:20547";

#[derive(Debug, Clone)]
struct Hello {
    id: u8,
    protocol: Vec<u8>,
    version: u32,
    checksum: u8
}

#[derive(Debug, Clone)]
struct Error {
    id: u8,
    message: Vec<u8>,
    checksum: u8
}

#[derive(Debug)]
enum Message {
    Hello(Hello),
    Error(Error)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start the TCP listener for incoming client connections
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(handle_client(stream));
    }
}

async fn handle_client(mut stream: TcpStream) {
    // Buffer to store incoming messages
    let mut buffer = [0u8; 4096];

    // Read from the stream
    // let n = stream.read(&mut buffer).await.expect("failed to read data");

    match stream.read_u8().await {
        Ok(_) => {}
        Err(_) => {}
    }

    // Handle the incoming data based on message type
    let response = match stream.read_u8().await.unwrap() {
        0x50 => handle_hello(&mut stream).await,
        // 0x58 => handle_site_visit(&buffer[1..n]).await,
        _ => {
            Message::Error(Error{
                id: 51,
                message: vec![0x62, 0x61, 0x64],
                checksum: 0x78,
            })
        }
    };

}

fn verify_message(message: Message) {

    let check = match message {
        Message::Hello(message) => {
            let check = [
                    (message.protocol.len() as u32).to_be_bytes(),
                    message.version.to_be_bytes(),
                ]
                .iter()
                .flatten()
                .chain(message.protocol.iter())
                .chain(std::iter::once(&message.id))
                .chain(std::iter::once(&message.checksum))
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
        Message::Error(message) => {
            let check = (message.message.len() as u32).to_be_bytes().iter()
                .chain(message.message.iter())
                .chain(std::iter::once(&message.id))
                .chain(std::iter::once(&message.checksum))
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
    };

}

async fn handle_hello(stream: &mut TcpStream) -> Message {
    let id:u8 = 0x50;
    let protocol_length = stream.read_u32().await.unwrap();
    let mut protocol = vec![0u8; protocol_length as usize];
    for _ in [..protocol_length]{
        protocol.push(stream.read_u8().await.unwrap());
    }
    let version = stream.read_u32().await.unwrap();
    let checksum = stream.read_u8().await.unwrap();
    let hello = Hello{id, protocol, version, checksum};
    Message::Hello(hello)

}

async fn handle_site_visit(data: &[u8]) {
    let site = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    println!("Received a SiteVisit for site: {}", site);

    // TODO: Decode the species data using the given specification

    // Check if we already have a connection to the authority server for the given site
    // If not, create a connection and request TargetPopulations
    // Compare the observed populations with the target populations
    // Create or delete policies as required
}

// Additional functions to decode messages, handle checksums, connect to the authority server, etc. should be added.
