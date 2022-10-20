extern crate core;

use std::collections::HashMap;
use tokio::net::{TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    println!("Ready to accept connections");
    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut data = HashMap::new();
            // In a loop, read data from the socket and write the data back.
            loop {
                let msg_type = socket.read_u8().await.unwrap();
                // println!("{:?}", msg_type as char);

                if msg_type == 73 {
                    do_insert(&mut socket, &mut data).await;
                } else if msg_type == 81 {
                    do_query(&mut socket, &data).await;
                } else {
                    return;
                }

            }
        });
    }
}

async fn do_insert(socket: &mut tokio::net::TcpStream, data: &mut HashMap<i32,i32>){
    let timestamp = socket.read_i32().await.unwrap();
    let price = socket.read_i32().await.unwrap();
    println!("Insert {:?}: {:?}", timestamp, price);
    data.insert(timestamp, price);
}

async fn do_query(socket: &mut tokio::net::TcpStream, data: &HashMap<i32,i32>){
    let min_time = socket.read_i32().await.unwrap();
    let max_time = socket.read_i32().await.unwrap();
    let mut total:i64 = 0;
    let mut value_count = 0;
    for (time, value) in data {
        if min_time <= *time && *time <= max_time {
            total += *value as i64;
            value_count += 1
        }
    }

    let result = match value_count {
        0 => 0,
        _ => total / value_count
    };

    println!("Insert {:?}: {:?}: {:?}", min_time, max_time, result);
    if let Err(e) = socket.write_i32(result as i32).await {
        eprintln!("failed to write to socket; err = {:?}", e);
        return;
    };
}
