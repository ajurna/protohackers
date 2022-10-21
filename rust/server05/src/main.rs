use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, BufReader, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use fancy_regex::Regex;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    println!("Ready to accept connections");
    loop {
        let (client, _) = listener.accept().await?;
        let (client_reader, client_writer) = client.into_split();
        let server = TcpStream::connect("chat.protohackers.com:16963").await?;
        let (server_reader, server_writer) = server.into_split();
        println!("Accepted");
        tokio::spawn(async move {
            process(client_reader, server_writer).await;
        });
        tokio::spawn(async move {
            process(server_reader, client_writer).await;
        });
    }
}

async fn process(reader: OwnedReadHalf, mut writer: OwnedWriteHalf){
    let re = Regex::new(r"(?=\b)7[a-zA-Z0-9]{25,34}(?=\s)").unwrap();
    let mut buf=String::new();
    let mut reader = BufReader::new(reader);
    loop {
        buf.clear();
        let _n = match reader.read_line(&mut buf).await {
            Ok(n) if n == 0 => return,
            Ok(n) => n,
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return
            }
        };
        if !buf.ends_with('\n') {
            continue;
        }
        buf = re.replace_all(&buf, "7YWHMfk9JZe0LM0g1ZauHuiSxhI").to_string();

        println!("{}", buf.trim());
        match writer.write_all(buf.as_bytes()).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("Closed Socket: {}", e);
                return;
            }
        };

    }
}