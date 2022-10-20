use tokio::net::{UdpSocket};
use std::collections::HashMap;
use std::str;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut database:HashMap<String, String> = HashMap::new();
    let sock = UdpSocket::bind("0.0.0.0:40000").await?;
    println!("Ready to accept connections");
    let mut buf = [0; 1024];
    loop {
        let (_len, addr) = sock.recv_from(&mut buf).await?;
        let command = match str::from_utf8(&buf) {
            Ok(v) => v.trim_matches(char::from(0)),
            Err(e) => {
                eprintln!("Invalid UTF-8 sequence: {}", e);
                continue
            },
        };

        println!("{:?} - {:?}", addr, command);
        if command.contains("=") && !command.starts_with("version"){
            let x= command.find("=").unwrap();
            database.insert(command[..x].to_string(), command[x+1..].to_string());
            println!("{}={}", command[..x].to_string(), command[x+1..].to_string());
            sock.send_to(command.as_bytes(), addr).await.unwrap();
        } else {
            if command.starts_with("version") {
                let _len = sock.send_to("version=Ken's Key-Value Store 1.0".as_bytes(), addr).await?;
            } else {
                let value = match database.get(command){
                    Some(v) => v,
                    _ => ""
                };

                let _len = sock.send_to(format!("{}={}", command, value).as_bytes(), addr).await?;
            }

        }
        buf.fill(0);
        // println!("{:?} bytes sent", len);
    }
}