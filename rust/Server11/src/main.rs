use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use enum_as_inner::EnumAsInner;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use dashmap::DashMap;

const AUTHORITY_SERVER: &str = "pestcontrol.protohackers.com:20547";

type SiteId = u32;

#[derive(Clone)]
struct Hello {
    id: u8,
    length: u32,
    protocol: Vec<u8>,
    version: u32,
    checksum: u8
}

impl fmt::Debug for Hello {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hello")
            .field("id", &self.id)
            .field("length", &self.length)
            // Custom formatting for the `protocol` field
            .field("protocol", &String::from_utf8_lossy(&self.protocol))
            .field("version", &self.version)
            .field("checksum", &self.checksum)
            .finish()
    }
}

#[derive(Clone)]
struct Error {
    id: u8,
    length: u32,
    message: Vec<u8>,
    checksum: u8
}
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("id", &self.id)
            .field("length", &self.length)
            // Custom formatting for the `protocol` field
            .field("message", &String::from_utf8_lossy(&self.message))
            .field("checksum", &self.checksum)
            .finish()
    }
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

#[derive(Clone)]
struct PopulationPolicy {
    species: Vec<u8>,
    min: u32,
    max: u32
}
impl fmt::Debug for PopulationPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PopulationPolicy")
            .field("species", &String::from_utf8_lossy(&self.species))
            .field("min", &self.min)
            .field("max", &self.max)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct TargetPopulations {
    id: u8,
    length: u32,
    site: u32,
    populations: Vec<PopulationPolicy>,
    checksum: u8,
}

#[derive(Clone)]
struct CreatePolicy {
    id: u8,
    length: u32,
    species: Vec<u8>,
    action: u8,
    checksum: u8
}
impl fmt::Debug for CreatePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let action_str = match self.action {
            0x90 => "cull",
            0xa0 => "conserve",
            _ => "unknown",
        };
        f.debug_struct("CreatePolicy")
            .field("id", &self.id)
            .field("length", &self.length)
            // Custom formatting for the `protocol` field
            .field("species", &String::from_utf8_lossy(&self.species))
            .field("action", &action_str)
            .field("checksum", &self.checksum)
            .finish()
    }
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

#[derive(Clone)]
struct PopulationCount {
    species: Vec<u8>,
    count: u32
}
impl fmt::Debug for PopulationCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PopulationCount")
            .field("species", &String::from_utf8_lossy(&self.species))
            .field("count", &self.count)
            .finish()
    }
}
#[derive(Debug, Clone)]
struct SiteVisit {
    id: u8,
    length: u32,
    site: u32,
    populations: Vec<PopulationCount>,
    checksum: u8
}

#[derive(Debug, Clone, EnumAsInner)]
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

#[derive(Debug, Clone)]
struct Policy {
    id: u32,
    action: u8
}

struct Target {
    min: u32,
    max: u32,
}

#[non_exhaustive]
struct PolicyAction;

impl PolicyAction {
    pub const CULL: u8 = 0x90;
    pub const CONSERVE: u8 = 0xa0;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // let e = create_hello();
    // println!("{e:?}");
    // let v = verify_checksum(&e);
    // println!("verified: {v:?}");
    // let s = serialise_message(&e);
    // let hex_string: String = s.iter()
    //     .map(|byte| format!("{:02X}", byte))
    //     .collect::<Vec<String>>()
    //     .join(" ");
    // println!("serialised: {hex_string:?}");
    // let w = String::from_utf8_lossy(&e.as_hello().unwrap().protocol);
    // println!("{w:?}");

    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    println!("Listening on port 40000");


    // let authorities:Arc<Mutex<HashMap<u32, TcpStream>>> = Arc::new(Mutex::new(HashMap::new()));

    let active_policies: DashMap<u32, Arc<Mutex<HashMap<String, Policy>>>> = DashMap::new();
    loop {
        let (stream, addr) = listener.accept().await?;
        let active_policies  = active_policies.clone();
        println!("Connection received: {:?}", addr);
        tokio::spawn(handle_client(stream, active_policies, addr));
    }
}

fn serialise_message(message: &Message) -> Vec<u8>{

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
        Message::Error(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend((message.message.len() as u32).to_be_bytes());
            bytes.extend(message.message.clone());
            bytes.push(message.checksum);
        }
        Message::OK(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.push(message.checksum);
        }
        Message::DialAuthority(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend(message.site.to_be_bytes());
            bytes.push(message.checksum)
        }
        Message::TargetPopulations(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend(message.site.to_be_bytes());
            bytes.extend((message.populations.len() as u32).to_be_bytes());
            for population in &message.populations {
                bytes.extend((population.species.len() as u32).to_be_bytes());
                bytes.extend(population.species.iter());
                bytes.extend(population.min.to_be_bytes());
                bytes.extend(population.max.to_be_bytes());
            }
            bytes.push(message.checksum)
        }
        Message::CreatePolicy(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend((message.species.len() as u32).to_be_bytes());
            bytes.extend(message.species.iter());
            bytes.extend([message.action, message.checksum]);
        }
        Message::DeletePolicy(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend(message.policy.to_be_bytes());
            bytes.push(message.checksum)
        }
        Message::PolicyResult(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend(message.policy.to_be_bytes());
            bytes.push(message.checksum)
        }
        Message::SiteVisit(message) => {
            bytes.push(message.id);
            bytes.extend(message.length.to_be_bytes());
            bytes.extend(message.site.to_be_bytes());
            bytes.extend((message.populations.len() as u32).to_be_bytes());
            for population in &message.populations {
                bytes.extend((population.species.len() as u32).to_be_bytes());
                bytes.extend(population.species.iter());
                bytes.extend(population.count.to_be_bytes());
            }
            bytes.push(message.checksum)
        }
        Message::NOOP => {}
    }
    bytes
}

async fn handle_client(
    mut stream: TcpStream,
    active_policies: DashMap<u32, Arc<Mutex<HashMap<String, Policy>>>>,
    addr: SocketAddr
) {
    // Buffer to store incoming messages

    // Read from the stream
    // let n = stream.read(&mut buffer).await.expect("failed to read data");
    let mut site_policies: HashMap<SiteId, HashMap<String, PopulationPolicy>> = HashMap::new();
    loop {
        // Handle the incoming data based on message type
        println!("{addr} Waiting for message");
        let message = get_message(&mut stream).await;
        println!("{addr} {message:?}");
        match message {
            Message::Hello(_) => {
                stream.write_all(&*serialise_message(&create_hello())).await.unwrap();
            }
            Message::Error(_) => {}
            Message::OK(_) => {}
            Message::DialAuthority(_) => {}
            Message::TargetPopulations(_) => {}
            Message::CreatePolicy(_) => {}
            Message::DeletePolicy(_) => {}
            Message::PolicyResult(_) => {}
            Message::SiteVisit(message) => {
                println!("{addr} Start Visit");
                let site_policies = match site_policies.get(&message.site) {
                    Some(policies) => {policies}
                    None => {
                        let (policies, _) = handshake_authority(&message.site).await;
                        let site = message.site.clone();
                        println!("{addr} {site} {policies:?}");
                        site_policies.insert(message.site.clone(), policies);
                        site_policies.get(&message.site).unwrap()
                    }
                };

                for population in message.populations{

                    let species_string = String::from_utf8_lossy(&population.species).to_string();
                    // println!("{addr}, {species_string}");
                    let species_policy = match site_policies.get(&species_string) {
                        None => {continue}
                        Some(policy) => {policy}
                    };


                    let active_site_policies = match active_policies.get_mut(&message.site) {
                        Some(active_policies) => {active_policies.clone()},
                        None => {
                            active_policies.insert(message.site.clone(), Arc::new(Mutex::new(HashMap::new())));
                            active_policies.get_mut(&message.site).unwrap().clone()
                        }
                    };
                    let mut active_site_policies = active_site_policies.lock().await;
                    match population.count {
                        x if x < species_policy.min => {
                            match active_site_policies.get(&species_string) {
                                None => {
                                    let new_active_policy = authority_create_policy(&message.site,population, PolicyAction::CONSERVE, &species_string, &addr).await;
                                    active_site_policies.insert(species_string.clone(), new_active_policy);
                                }
                                Some(active_site_policy) => {
                                    if active_site_policy.action == PolicyAction::CULL {
                                        authority_delete_policy(&message.site, active_site_policy, &species_string, &addr).await;
                                        let new_active_policy = authority_create_policy(&message.site, population, PolicyAction::CONSERVE, &species_string, &addr).await;
                                        active_site_policies.insert(species_string.clone(), new_active_policy);
                                    }
                                }
                            }
                        }
                        x if x >= species_policy.min && x <= species_policy.max  => {
                            match active_site_policies.get(&species_string) {
                                None => {}
                                Some(active_site_policy) => {
                                    authority_delete_policy(&message.site, active_site_policy, &species_string, &addr).await;
                                    active_site_policies.remove(&species_string).unwrap();
                                }
                            }

                        }
                        x if x > species_policy.max => {
                            match active_site_policies.get(&species_string) {
                                None => {
                                    let new_active_policy = authority_create_policy(&message.site, population, PolicyAction::CULL, &species_string, &addr).await;
                                    active_site_policies.insert(species_string.clone(), new_active_policy);
                                }
                                Some(active_site_policy) => {
                                    if active_site_policy.action == PolicyAction::CONSERVE {
                                        authority_delete_policy(&message.site, active_site_policy, &species_string, &addr).await;
                                        let new_active_policy = authority_create_policy(&message.site, population, PolicyAction::CULL, &species_string, &addr).await;
                                        active_site_policies.insert(species_string.clone(), new_active_policy);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                println!("{addr} End Visit");
            }
            Message::NOOP => {
                return;
            }
        }
    }

}

async fn authority_create_policy(site_id: &u32, population: PopulationCount, policy_action: u8, species: &String, addr: &SocketAddr) -> Policy {
    println!("{addr} create policy {site_id} {species}, {policy_action}");
    let (_, mut authority) = handshake_authority(site_id).await;

    let new_policy = create_create_policy(population.species.clone(), policy_action);
    authority.write_all(&serialise_message(&new_policy)).await.unwrap();
    authority.flush().await.unwrap();
    let policy_result = get_message(&mut authority).await;
    let policy_result = policy_result.as_policy_result().expect("Create Policy Failed");
    Policy { id: policy_result.policy, action: policy_action }
}

async fn authority_delete_policy(
    site_id: &u32,
    active_site_policy: &Policy,
    species: &String,
    addr: &SocketAddr
) {
    println!("{addr} delete policy {site_id} {species} {active_site_policy:?}");
    let (_, mut authority) = handshake_authority(site_id).await;
    let delete_policy = create_delete_policy(active_site_policy.id);
    authority.write_all(&serialise_message(&delete_policy)).await.unwrap();
    authority.flush().await.unwrap();
    let ok = get_message(&mut authority).await;
    let _ok = ok.as_ok().expect("Policy Delete Error");
}

async fn handshake_authority(site_id: &u32) -> (HashMap<String, PopulationPolicy>, TcpStream) {
    let mut authority = TcpStream::connect(AUTHORITY_SERVER).await.unwrap();
    let hello = create_hello();
    authority.write_all(&*serialise_message(&hello)).await.unwrap();

    let _response = get_message(&mut authority).await;

    let dial_authority = create_dial_authority(site_id);
    let dial_authority = serialise_message(&dial_authority);
    authority.write_all(&*dial_authority).await.unwrap();

    let response = get_message(&mut authority).await;
    let target_populations = response.as_target_populations().unwrap();
    let mut policies = HashMap::new();
    for policy in &target_populations.populations {
        policies.insert(String::from_utf8_lossy(&policy.species).to_string(), policy.clone());
    }
    (policies, authority)
}


fn verify_message(message: &Message) -> bool {
    verify_checksum(&message)
}



async fn get_message(stream: &mut TcpStream) -> Message {
    let message_type ={
         match stream.read_u8().await {
            Ok(byte) => { byte }
            Err(_) => { return Message::NOOP; }
        }
    };

    let message = match message_type {
        0x50 => handle_hello(stream).await,
        0x51 => handle_error(stream).await,
        0x52 => handle_ok(stream).await,
        0x53 => handle_dial_authority(stream).await,
        0x54 => handle_target_populations(stream).await,
        0x55 => handle_create_policy(stream).await,
        0x56 => handle_delete_policy(stream).await,
        0x57 => handle_policy_result(stream).await,
        0x58 => handle_site_visit(stream).await,
        _ => {create_error("unknown message type")}
    };
    if verify_message(&message){
        message
    } else {
        eprintln!("{message:?}");
        create_error("verification failed")
    }

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
    let id = 0x51;
    let length = stream.read_u32().await.unwrap();
    let message_length = stream.read_u32().await.unwrap();
    let mut message = Vec::new();
    for _ in 0..message_length {
        message.push(stream.read_u8().await.unwrap());
    }
    let checksum = stream.read_u8().await.unwrap();
    Message::Error(Error{
        id,
        length,
        message,
        checksum,
    })
}
async fn handle_ok(stream: &mut TcpStream) -> Message {
    let id = 0x52;
    let length = stream.read_u32().await.unwrap();
    let checksum = stream.read_u8().await.unwrap();
    Message::OK(OK{
        id,
        length,
        checksum,
    })
}
async fn handle_dial_authority(stream: &mut TcpStream) -> Message {
    let id = 0x53;
    let length = stream.read_u32().await.unwrap();
    let site = stream.read_u32().await.unwrap();
    let checksum = stream.read_u8().await.unwrap();
    Message::DialAuthority(DialAuthority{
        id,
        length,
        site,
        checksum,
    })
}
async fn handle_target_populations(stream: &mut TcpStream) -> Message {
    let id: u8 = 0x54;
    let length = stream.read_u32().await.unwrap();
    let site = stream.read_u32().await.unwrap();
    let population_count = stream.read_u32().await.unwrap();
    let mut populations = Vec::new();
    for _ in 0..population_count {
        let species_length = stream.read_u32().await.unwrap();
        let mut species = Vec::new();
        for _ in 0..species_length {
            species.push(stream.read_u8().await.unwrap());
        }
        let min = stream.read_u32().await.unwrap();
        let max = stream.read_u32().await.unwrap();
        populations.push(PopulationPolicy{
            species,
            min,
            max,
        })
    }
    let checksum = stream.read_u8().await.unwrap();
    Message::TargetPopulations(TargetPopulations{
        id,
        site,
        length,
        populations,
        checksum,
    })
}
async fn handle_create_policy(stream: &mut TcpStream) -> Message {
    let id = 0x55;
    let length = stream.read_u32().await.unwrap();
    let species_length  = stream.read_u32().await.unwrap();
    let mut species = Vec::new();
    for _ in 0..species_length {
        species.push(stream.read_u8().await.unwrap())
    }
    let action = stream.read_u8().await.unwrap();
    let checksum = stream.read_u8().await.unwrap();
    Message::CreatePolicy(CreatePolicy{
        id,
        length,
        species,
        action,
        checksum,
    })
}
async fn handle_delete_policy(stream: &mut TcpStream) -> Message {
    let id = 0x56;
    let length = stream.read_u32().await.unwrap();
    let policy = stream.read_u32().await.unwrap();
    let checksum = stream.read_u8().await.unwrap();
    Message::DeletePolicy(DeletePolicy{
        id,
        length,
        policy,
        checksum,
    })
}
async fn handle_policy_result(stream: &mut TcpStream) -> Message {
    let id = 0x57;
    let length = stream.read_u32().await.unwrap();
    let policy = stream.read_u32().await.unwrap();
    let checksum = stream.read_u8().await.unwrap();
    Message::PolicyResult(PolicyResult{
        id,
        length,
        policy,
        checksum,
    })
}
async fn handle_site_visit(stream: &mut TcpStream) -> Message {
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
    Message::SiteVisit(SiteVisit{
        id,
        length,
        site,
        populations,
        checksum
    })
}


fn create_hello() -> Message {
    let hello = Message::Hello(Hello{
        id: 0x50,
        length: 0x19,
        protocol: vec![0x70, 0x65, 0x73, 0x74, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c],
        version: 0x01,
        checksum: 0xce,
    });
    hello
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

fn create_ok() -> Message {
    let id = 0x52;
    let length = 6;
    let checksum = 0xa8;
    Message::OK(OK{
        id,
        length,
        checksum,
    })
}

fn create_dial_authority(site: &u32) -> Message{
    let id: u8 = 0x53;
    let length: u32 = 10;
    let site = site.clone();

    let checksum = 0u8.wrapping_sub([
            length.to_be_bytes(),
            site.to_be_bytes(),
        ]
        .iter()
        .flatten()
        .chain(std::iter::once(&id))
        .fold(0u8, |acc, &byte| acc.wrapping_add(byte)));
    let dial_authority = Message::DialAuthority(DialAuthority{
        id,
        length,
        site,
        checksum,
    });
    dial_authority
}

fn create_target_populations(site: u32, populations: Vec<PopulationPolicy>) -> Message {
    let id = 0x54;
    let mut length: u32 = 14;
    let mut checksum= [
        length.to_be_bytes(),
        (populations.len() as u32).to_be_bytes()
    ]
        .iter()
        .flatten()
        .chain([&id])
        .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
    for policy in populations.iter() {
        length += policy.species.len() as u32 + 12;
        checksum = checksum.wrapping_add([
            (policy.species.len() as u32).to_be_bytes(),
            policy.min.to_be_bytes(),
            policy.max.to_be_bytes()
        ]
            .iter()
            .flatten()
            .chain(policy.species.iter())
            .fold(0u8, |acc, &byte| acc.wrapping_add(byte)));
    }
    checksum = 0u8.wrapping_sub(checksum);
    Message::TargetPopulations(TargetPopulations{
        id,
        length,
        site,
        populations,
        checksum,
    })
}

fn create_create_policy(species: Vec<u8>, action: u8) -> Message{
    let id = 0x55;
    let length = species.len() as u32 + 11;
    let mut checksum = [
        length.to_be_bytes(),
        (species.len() as u32).to_be_bytes()
    ]
        .iter()
        .flatten()
        .chain([&id, &action])
        .chain(species.iter())
        .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
    checksum = 0u8.wrapping_sub(checksum);
    Message::CreatePolicy(CreatePolicy{
        id,
        length,
        species,
        action,
        checksum,
    })
}

fn create_delete_policy(policy: u32) -> Message{
    let id = 0x56;
    let length:u32 = 10;
    let mut checksum = [
        length.to_be_bytes(),
        policy.to_be_bytes()
    ]
        .iter()
        .flatten()
        .chain([&id])
        .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
    checksum = 0u8.wrapping_sub(checksum);
    Message::DeletePolicy(DeletePolicy{
        id,
        length,
        policy,
        checksum,
    })
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
                .chain([&message.id, &message.checksum])
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
                .chain([&message.id, &message.checksum])
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
        Message::OK(message) => {
            let check = message.length.to_be_bytes()
                .iter()
                .chain([&message.id, &message.checksum])
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
        Message::DialAuthority(message) => {
            let check = [
                message.length.to_be_bytes(),
                message.site.to_be_bytes(),
            ]
                .iter()
                .flatten()
                .chain([&message.id, &message.checksum])
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
        Message::TargetPopulations(message) => {
            let mut check = [
                message.length.to_be_bytes(),
                message.site.to_be_bytes(),
                (message.populations.len() as u32).to_be_bytes()
            ]
                .iter()
                .flatten()
                .chain([&message.id, &message.checksum])
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));

            check = check.wrapping_add(message.populations
                .iter()
                .map(| p | [
                    (p.species.len() as u32).to_be_bytes(),
                    p.min.to_be_bytes(),
                    p.max.to_be_bytes()
                ]
                    .iter()
                    .flatten()
                    .chain(p.species.iter())
                    .fold(0u8, |acc, &byte| acc.wrapping_add(byte)))
                .fold(0u8, |acc, byte| acc.wrapping_add(byte)));
            check
        }
        Message::CreatePolicy(message) => {
            let check = [
                message.length.to_be_bytes(),
                (message.species.len() as u32).to_be_bytes(),
            ]
                .iter()
                .flatten()
                .chain(message.species.iter())
                .chain([&message.id, &message.checksum, &message.action])
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
        Message::DeletePolicy(message) => {
            let check = [
                message.length.to_be_bytes(),
                message.policy.to_be_bytes()
            ]
                .iter()
                .flatten()
                .chain([&message.id, &message.checksum])
                .fold(0u8, |acc, &byte| acc.wrapping_add(byte));
            check
        }
        Message::PolicyResult(message) => {
            let check = [
                message.length.to_be_bytes(),
                message.policy.to_be_bytes()
            ]
                .iter()
                .flatten()
                .chain([&message.id, &message.checksum])
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
                .chain([&message.id, &message.checksum])
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
        Message::NOOP => {1}
    };
    // println!("check: {}", check);
    check == 0
}