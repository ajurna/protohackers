use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const AUTHORITY_SERVER: &str = "pestcontrol.protohackers.com:20547";

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
    let n = stream.read(&mut buffer).await.expect("failed to read data");

    // Handle the incoming data based on message type
    match buffer[0] {
        0x50 => handle_hello(&buffer[1..n]),
        0x58 => handle_site_visit(&buffer[1..n]).await,
        _ => {
            println!("Unsupported message type: {}", buffer[0]);
        }
    }
}

fn handle_hello(data: &[u8]) {
    println!("Received a Hello message with protocol version: {}", u32::from_be_bytes([data[11], data[12], data[13], data[14]]));
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
