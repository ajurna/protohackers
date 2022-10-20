use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, BufReader, AsyncWriteExt};
use std::collections::HashMap;
use std::sync::{Arc};
use tokio::net::tcp::OwnedWriteHalf;
use itertools::free::join;
use tokio::sync::Mutex;

type ConnectionHandler = Arc<Mutex<HashMap<String, OwnedWriteHalf>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let connections:ConnectionHandler = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    println!("Ready to accept connections");
    loop {
        let (socket, _) = listener.accept().await?;
        let connections = connections.clone();
        println!("Accepted");
        tokio::spawn(async move {
            process(socket, connections).await;
        });
    }
}

async fn process(socket: TcpStream, connections: ConnectionHandler) {
    let mut message = String::new();
    let mut username = String::new();
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);

    writer.write_all("Welcome to budgetchat! What shall I call you?\n".as_bytes()).await.unwrap();

    let _n = match reader.read_line(&mut username).await {
        Ok(n) if n == 0 => {
            println!("Disconnected: without username");
            return
        },
        Ok(n) => n,
        Err(e) => {
            eprintln!("Error: {:?}", e);
            return
        }
    };

    username = username.trim().to_string();
    if !username.chars().all(char::is_alphanumeric) && username.len() > 0 {
        println!("User: {} invalid and kicked", username);
        writer.shutdown().await.unwrap();
        return ;
    }
    println!("User: {} joined", username);
    broadcast(&username, format!("* {} has entered the room\n", username), &connections).await;
    {
        let mut connections = connections.lock().await;
        let users = join(connections.keys().into_iter(), ", ");
        let message_to_send = format!("* The room contains: {}\n", users);
        writer.write_all(message_to_send.as_bytes()).await.unwrap();
        connections.insert(username.clone(), writer);
    }
    loop {
        message.clear();
        let _n = match reader.read_line(&mut message).await {
            Ok(n) if n == 0 => {
                broadcast(&username, format!("* {} has left the room\n", username), &connections).await;
                println!("Disconnected: {:?}", username);
                let mut connections = connections.lock().await;
                connections.remove(&username);
                return
            },
            Ok(n) => n,
            Err(e) => {
                broadcast(&username, format!("* {} has left the room\n", username), &connections).await;
                eprintln!("Error: {:?}", e);
                let mut connections = connections.lock().await;
                connections.remove(&username);
                return
            }
        };
        message = message.trim().to_string();
        if message.len() > 0 {
            broadcast(&username, format!("[{}] {}\n", username, message), &connections).await;
        }
    }
}

async fn broadcast(source_user: &String, message:String, connections: &ConnectionHandler) {
    println!("Broadcast: {:?}", message);
    let mut connections = connections.lock().await;
    for (user, writer) in connections.iter_mut() {
        if user != source_user {
            writer.write_all(message.as_bytes()).await.unwrap();
        }
    }
}