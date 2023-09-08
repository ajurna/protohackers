use tokio::net::{UdpSocket};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::net::{SocketAddr};
use std::str;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::Command::Connect;




#[derive(Debug, Clone)]
struct CommandData {
    session: u32,
    ord: u32,
    message: Vec<u8>
}
#[derive(Debug, Clone)]
enum Command {
    Connect(CommandData),
    Data(CommandData),
    Ack(CommandData),
    Close(CommandData),
}


impl Command {
    pub fn new(value: Vec<u8>) -> Option<Command> {
        let slash = '/' as u8;
        let connect_text = "connect".as_bytes();
        let data_text = "data".as_bytes();
        let ack_text = "ack".as_bytes();
        let close_text = "close".as_bytes();
        let empty_message = vec![];

        match value.first() {
            Some(s) if s == &slash => (),
            _ => return None
        }
        match value.last() {
            Some(s) if s == &slash => (),
            _ => return None
        }
        let command_end = match value[1..].iter().position(|x| x == &slash){
            None => return None,
            Some(n) => n+1
        };
        let session_end = match value[command_end+1..].iter().position(|x| x == &slash){
            None => return None,
            Some(n) => n+command_end+1
        };

        let session = match str::from_utf8(&value[command_end + 1..session_end]) {
            Ok(s) => match s.parse::<u32>() {
                Ok(n) if n < 2147483648 => n,
                _ =>return None,
            },
            Err(_) => return None
        };
        if connect_text == &value[1..command_end] {
            return Some(Command::Connect(CommandData{
                session,
                ord: 0,
                message: empty_message,
            }))
        }
        if close_text == &value[1..command_end] {
            return Some(Command::Close(CommandData{
                session,
                ord: 0,
                message: empty_message,
            }))
        }

        let ord_end = match value[session_end+1..].iter().position(|x| x == &slash){
            None => return None,
            Some(n) => n+session_end+1
        };

        let ord = match str::from_utf8(&value[session_end + 1..ord_end]) {
            Ok(s) => match s.parse::<u32>() {
                Ok(n) if n < 2147483648 => n,
                _ =>return None,
            },
            Err(_) => return None
        };
        if ack_text == &value[1..command_end] {
            return Some(Command::Ack(CommandData{
                session,
                ord,
                message: empty_message,
            }))
        }
        // let message = value[ord_end+1..value.len()-1].to_vec();
        let mut message = vec![];
        for i in ord_end+1..value.len()-1 {
            match value[i] {
                92 => {
                    if value[i+1] == 47 || value[i+1] == 92 {
                        continue
                    }
                    message.push(value[i]);
                },
                _ => {message.push(value[i]);}
            }
        }

        if data_text == &value[1..command_end] {
            return Some(Command::Data(CommandData{
                session,
                ord,
                message,
            }))
        }

        println!("");
        println!("{:?}: {:?}", value, session_end);
        println!("{:?}: {:?}", &value[ord_end+1..value.len()-1], connect_text);
        println!("");
        None
    }
    fn session_id (self) -> u32 {
        let mut n = 0u32;
        return match self {

            Connect(d) => {
                n = d.session;
                n
            },
            Command::Data(d) => {
                n = d.session;
                n
            },
            Command::Ack(d) => {
                n = d.session;
                n
            },
            Command::Close(d) => {
                n = d.session;
                n
            }
        }
    }
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // let command = Command::new("/connect/123/".as_bytes().to_vec()).unwrap();
    // println!("{:?}", command);
    // let command = Command::new("/close/123/".as_bytes().to_vec()).unwrap();
    // println!("{:?}", command);
    // let command = Command::new("/ack/123/500/".as_bytes().to_vec()).unwrap();
    // println!("{:?}", command);
    // let command = Command::new("/data/123/0/Hello/".as_bytes().to_vec()).unwrap();
    // println!("{:?}", command);
    // let command = Command::new(r#"/data/123/0/foo\/bar\\baz/"#.as_bytes().to_vec()).unwrap();
    // println!("{:?}", command);
    // println!("{:?}", str::from_utf8(command));
    // match command {
    //     Command::Data(d) => {println!("{:?}", str::from_utf8(&d.message).unwrap())}
    //     _ => {}
    // }

    let mut sessions: HashMap<u32, Sender<Command>> = HashMap::new();
    let sock = UdpSocket::bind("0.0.0.0:54321").await?;
    println!("Ready to accept connections");
    let mut buf = [0; 1024];
    loop {

        let (len, addr) = sock.recv_from(&mut buf).await?;
        let action = match Command::new(buf.split_at(len).0.to_vec()) {
            Some(command)=> command,
            None => {
                println!("Error {:?}", buf.split_at(len).0);
                continue
            }
        };
        println!("{:?}", &action);

        let session_id = action.clone().session_id();

        match sessions.get(&session_id) {
            None => {
                let (tx, mut rx): (Sender<Command>, Receiver<Command>) = mpsc::channel(200);
                let tx = sessions.insert(session_id, tx).unwrap();
                tx.send(action).await.expect("Sender broken");
                tokio::spawn( async move {
                    session(addr, rx, session_id).await
                });
            }
            Some(tx) => { tx.send(action).await.expect("Sender broken?");}
        }


    }
}

async fn session(addr: SocketAddr, mut queue: Receiver<Command>, session_id: u32) {
    let sock = UdpSocket::bind("0.0.0.0:0").await.expect("Failed to bind sender");
    let messages:HashMap<u32, Vec<u8>> = HashMap::new();
    let closed = false;
    let data_acked: u32 = 0;
    loop {
        sleep(Duration::from_secs(1)).await;
        let processing_messages = true;

        while processing_messages{
            match queue.try_recv() {
                Ok(action) => {
                    match action {
                        Command::Connect(command) => {
                            let message = format!("/ack/{session_id}/{data_acked}/");
                            send_message(&sock, &addr, message).await;
                        },
                        Command::Data(command) => {

                        },
                        Command::Ack(command) => {
                            if closed {
                                let message = format!("/close/{session_id}/");
                                send_message(&sock, &addr, message).await;
                                continue
                            }
                            
                        },
                        Command::Close(command) => {
                            let message = format!("/close/{session_id}/");
                            send_message(&sock, &addr, message).await;
                        }
                    }
                }
                Err(TryRecvError::Empty) => {}
                Err(_) => {}
            }
        }
    }
}

async fn send_message(sock: &UdpSocket, addr: &SocketAddr, message: String){
    sock.send_to(message.as_bytes(), addr);
}