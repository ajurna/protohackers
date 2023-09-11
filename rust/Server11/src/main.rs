use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const AUTHORITY_SERVER: &str = "pestcontrol.protohackers.com:20547";

#[derive(Debug, Clone)]
struct Hello {
    id: u8,
    length: u32,
    protocol: Vec<u8>,
    version: u32,
    checksum: u8
}

#[derive(Debug, Clone)]
struct Error {
    id: u8,
    length: u32,
    message: Vec<u8>,
    checksum: u8
}

#[derive(Debug, Clone)]
struct OK {
    id: u8,
    length: u32,
    checksum: u8
}

#[derive(Debug, Clone)]
struct DialAuthority {
    id: u8,
    length: u32,
    site: u32,
    checksum: u8
}

#[derive(Debug, Clone)]
struct PopulationPolicy {
    species: Vec<u8>,
    min: u32,
    max: u32
}

#[derive(Debug, Clone)]
struct TargetPopulations {
    id: u8,
    length: u32,
    populations: Vec<PopulationPolicy>,
    checksum: u8,
}

#[derive(Debug, Clone)]
struct CreatePolicy {
    id: u8,
    length: u32,
    species: Vec<u8>,
    action: u8,
    checksum: u8
}

#[derive(Debug, Clone)]
struct DeletePolicy {
    id: u8,
    length: u32,
    policy: u32,
    checksum: u8
}

#[derive(Debug, Clone)]
struct PolicyResult {
    id: u8,
    length: u32,
    policy: u32,
    checksum: u8
}

#[derive(Debug, Clone)]
struct PopulationCount {
    species: Vec<u8>,
    count: u32
}
#[derive(Debug, Clone)]
struct SiteVisit {
    id: u8,
    length: u32,
    site: u32,
    populations: Vec<PopulationCount>,
    checksum: u8
}

#[derive(Debug, Clone)]
enum Message {
    Hello(Hello),
    Error(Error),
    OK(OK),
    DialAuthority(DialAuthority),
    TargetPopulations(TargetPopulations),
    CreatePolicy(CreatePolicy),
    DeletePolicy(DeletePolicy),
    PolicyResult(PolicyResult),
    SiteVisit(SiteVisit),
    NOOP
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start the TCP listener for incoming client connections


    // let test = verify_message(&message);
    //
    // println!("{}", test);
    // let serialised = serialise_message(&message).await;
    // let hex_string: String = serialised.iter()
    //     .map(|byte| format!("{:02X}", byte))
    //     .collect::<Vec<String>>()
    //     .join(" ");
    // println!("{}", hex_string);

    let e = create_error("bad");
    println!("{e:?}");
    let v = verify_checksum(&e);
    println!("{v:?}");
    let listener = TcpListener::bind("0.0.0.0:40000").await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Connection received: {:?}", addr);
        tokio::spawn(handle_client(stream));
    }
}

async fn serialise_message(message: &Message) -> Vec<u8>{

    let mut bytes:Vec<u8> = Vec::new();
    match message {
        Message::Hello(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend((message.protocol.len() as u32).to_be_bytes());
            bytes.extend(message.protocol.clone());
            bytes.extend(message.version.to_be_bytes());
            bytes.push(message.checksum);

        }
        Message::Error(_) => {}
        Message::OK(_) => {}
        Message::DialAuthority(_) => {}
        Message::TargetPopulations(_) => {}
        Message::CreatePolicy(_) => {}
        Message::DeletePolicy(_) => {}
        Message::PolicyResult(_) => {}
        Message::SiteVisit(_) => {}
        Message::NOOP => {}
    }
    bytes
}

async fn handle_client(mut stream: TcpStream) {
    // Buffer to store incoming messages
    let hello: Hello = Hello{
        id: 0x50,
        length: 0x19,
        protocol: vec![0x70, 0x65, 0x73, 0x74, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c],
        version: 0x01,
        checksum: 0xce,
    };
    let hello_message = Message::Hello(hello);
    let authorties = Arc::new(Mutex::new(HashMap::new()));
    // Read from the stream
    // let n = stream.read(&mut buffer).await.expect("failed to read data");

    loop {
        // Handle the incoming data based on message type
        let message = match stream.read_u8().await.unwrap() {
            0x50 => handle_hello(&mut stream).await,
            0x51 => handle_error(&mut stream).await,
            0x52 => handle_ok(&mut stream).await,
            0x53 => handle_dial_authority(&mut stream).await,
            0x54 => handle_target_populations(&mut stream).await,
            0x55 => handle_create_policy(&mut stream).await,
            0x56 => handle_delete_policy(&mut stream).await,
            0x57 => handle_policy_result(&mut stream).await,
            0x58 => handle_site_visit(&mut stream).await,
            _ => {
                Message::Error(Error {
                    id: 51,
                    length: 0x0d,
                    message: vec![0x62, 0x61, 0x64],
                    checksum: 0x78,
                })
            }
        };
        println!("{:?}", message);
        match message {
            Message::Hello(_) => {
                let response = serialise_message(&hello_message).await;
                stream.write_all(&*response).await.unwrap();
            }
            Message::Error(_) => {}
            Message::OK(_) => {}
            Message::DialAuthority(_) => {}
            Message::TargetPopulations(_) => {}
            Message::CreatePolicy(_) => {}
            Message::DeletePolicy(_) => {}
            Message::PolicyResult(_) => {}
            Message::SiteVisit(message) => {
                let mut authorties = authorties.lock().await;
                let mut authority =  match authorties.get_mut(&message.site) {
                    Some(authority) => {authority},
                    _ => {

                        let mut authority = TcpStream::connect(AUTHORITY_SERVER).await.unwrap();
                        authorties.insert(&message.site, authority).unwrap();
                    }
                };
            }
            Message::NOOP => { println!("NOOP happened") }
        }
    }

}

fn verify_message(message: &Message) -> bool {
    verify_checksum(&message)
}

fn verify_checksum(message: &Message) -> bool{
    let check = match message {
        Message::Hello(message) => {
            let check = [
                    message.length.to_be_bytes(),
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
            let check = [
                    message.length.to_be_bytes(),
                    (message.message.len() as u32).to_be_bytes(),
                ].iter()
                .flatten()
                .chain(message.message.iter())
                .chain(std::iter::once(&message.id))
                .chain(std::iter::once(&message.checksum))
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
        Message::SiteVisit(message) => {
            let mut check = [
                    message.length.to_be_bytes(),
                    message.site.to_be_bytes(),
                    (message.populations.len() as u32).to_be_bytes()
                ].iter()
                .flatten()
                .chain(std::iter::once(&message.id))
                .chain(std::iter::once(&message.checksum))
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));

            check = check.wrapping_add(message.populations
                .iter()
                .map(| p | [
                        (p.species.len() as u32).to_be_bytes(),
                        p.count.to_be_bytes()
                    ]
                    .iter()
                    .flatten()
                    .chain(p.species.iter())
                    .fold(0u8, |acc, &byte| acc.wrapping_add(byte)))
                .fold(0u8, |acc, byte| acc.wrapping_add(byte)));
            check
        }
        _ => {1}
    };
    println!("check: {}", check);
    check == 0
}

async fn handle_hello(stream: &mut TcpStream) -> Message {
    let id:u8 = 0x50;
    let length = stream.read_u32().await.unwrap();
    let protocol_length = stream.read_u32().await.unwrap();
    let mut protocol = Vec::new();
    for _ in 0..protocol_length{
        protocol.push(stream.read_u8().await.unwrap());
    }
    let version = stream.read_u32().await.unwrap();
    let checksum = stream.read_u8().await.unwrap();
    let message = Message::Hello(Hello{id, length, protocol, version, checksum});
    // println!("{:?}", message);
    if verify_message(&message){
        return message
    }
    Message::Error(Error{
        id: 51,
        length: 0x0d,
        message: vec![0x62, 0x61, 0x64],
        checksum: 0x78,
    })
}

async fn handle_error(stream: &mut TcpStream) -> Message {
    println!("Handle Error called");
    Message::NOOP
}
async fn handle_ok(stream: &mut TcpStream) -> Message {
    println!("Handle ok called");
    Message::NOOP
}
async fn handle_dial_authority(stream: &mut TcpStream) -> Message {
    println!("Handle dial authority called");
    Message::NOOP
}
async fn handle_target_populations(stream: &mut TcpStream) -> Message {
    println!("Handle target populations called");
    Message::NOOP
}
async fn handle_create_policy(stream: &mut TcpStream) -> Message {
    println!("Handle create policy called");
    Message::NOOP
}
async fn handle_delete_policy(stream: &mut TcpStream) -> Message {
    println!("Handle delete policy called");
    Message::NOOP
}
async fn handle_policy_result(stream: &mut TcpStream) -> Message {
    println!("Handle policy result called");
    Message::NOOP
}
async fn handle_site_visit(stream: &mut TcpStream) -> Message {
    println!("Handle site visit called");
    let id:u8 = 0x58;
    let length = stream.read_u32().await.unwrap();
    let site = stream.read_u32().await.unwrap();
    let population_count = stream.read_u32().await.unwrap();
    let mut populations = Vec::new();
    for _ in 0..population_count {
        let species_length = stream.read_u32().await.unwrap();
        let mut species = Vec::new();
        for _ in 0..species_length {
            species.push(stream.read_u8().await.unwrap())
        }
        let count = stream.read_u32().await.unwrap();
        populations.push(PopulationCount{species, count });
    }
    let checksum = stream.read_u8().await.unwrap();
    let site_visit = Message::SiteVisit(SiteVisit{id, length, site, populations, checksum});

    println!("{site_visit:?}");
    if verify_message(&site_visit) {
        return site_visit
    }
    Message::NOOP
}

fn create_error(msg: &str) -> Message {
    let id:u8 = 0x51;
    let message = msg.as_bytes().to_vec();
    let length: u32 = 10 + message.len() as u32;
    let checksum = 0u8.wrapping_sub([
        length.to_be_bytes(),
        (message.len() as u32).to_be_bytes(),
    ]
        .iter()
        .flatten()
        .chain(message.iter())
        .chain(std::iter::once(&id))
        .fold(0u8, |acc, &byte| acc.wrapping_add(byte)));

    let error = Error{id: 0x51, length, message, checksum};
    Message::Error(error)
}
