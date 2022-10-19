extern crate core;

use std::string::String;
use tokio::net::{TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde_json;
use serde::{Deserialize, Serialize};
use primal::is_prime;


#[derive(Serialize, Deserialize)]
struct Response {
    method: String,
    prime: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    println!("Ready to accept connections");
    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut message_to_parse: Vec<u8>;
            let mut full_data: Vec<u8> = Vec::new();
            let mut buf = [0; 1024];
            let mut processing: bool;
            // In a loop, read data from the socket and write the data back.
            loop {
                let _n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };
                processing = true;
                while processing {
                    full_data.extend(buf.iter().filter(|&c| c != &u8::MIN));
                    buf.fill(0);
                    let new_line_index = match full_data.iter().position(|&c| c == 10) {
                        Some(new_line_index) => new_line_index,
                        None => {
                            processing=false;
                            continue
                        }
                    };

                    if new_line_index == 0 {
                        processing = false;
                        continue;
                    }
                    message_to_parse = full_data[0..=new_line_index].to_vec();
                    println!("message_to_parse: {:?}", String::from_utf8_lossy(&message_to_parse));
                    full_data = full_data[new_line_index + 1..].to_vec();

                    processing = process_message(&mut socket, message_to_parse).await;

                }
            }
        });
    }
}

async fn process_message(mut socket: &mut tokio::net::TcpStream, message_to_parse: Vec<u8>) -> bool{
    let json_default: serde_json::Value = serde_json::from_slice(&"[]".as_bytes()[..]).unwrap();
    let number_is_prime: bool;
    let mut invalid_data = false;
    // let data_as_str = String::from_utf8_lossy(&full_data).into_owned().as_str();
    let v: serde_json::Value = match serde_json::from_slice(&message_to_parse.as_slice()) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error {:?}", e);
            eprintln!("Message: {:?}", String::from_utf8_lossy(&message_to_parse));
            invalid_data = true;
            json_default.clone()
        },
    };

    let method_correct = v["method"] == "isPrime";
    if v["number"].is_number() {
        number_is_prime = match v["number"].as_u64() {
            Some(number) => is_prime(number),
            None => false,
        };
    } else {
        send_message(&mut socket, "Number is not Number\n").await;
        socket.flush().await.unwrap();
        socket.shutdown().await.unwrap();
        return false;
    }

    if invalid_data {
        send_message(&mut socket, "Data Invalid\n").await;
        socket.flush().await.unwrap();
        socket.shutdown().await.unwrap();
        false
    } else if !method_correct {
        send_message(&mut socket, "Method incorrect\n").await;
        socket.flush().await.unwrap();
        socket.shutdown().await.unwrap();
        false
    } else {
        let response_struct = Response { method: "isPrime".to_owned(), prime: number_is_prime };
        let response = serde_json::to_string(&response_struct).unwrap() + "\n";
        send_message(&mut socket, response.as_str()).await;
        true
    }
}

async fn send_message(socket: &mut tokio::net::TcpStream, string_to_send: &str){
    println!("Response: {:?}", string_to_send);
    if let Err(e) = socket.write_all(string_to_send.as_bytes()).await {
        eprintln!("failed to write to socket; err = {:?}", e);
        return;
    }
}