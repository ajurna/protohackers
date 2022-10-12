use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:40000").await?;
    println!("Ready to accept connections");
    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf: Vec<u8> = Vec::new();
            // In a loop, read data from the socket and write the data back.
            loop {
                let _n = match socket.read_to_end(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                let message = String::from_utf8_lossy(&buf);
                println!("Message: {:?}", message);

                // Write the data back
                if let Err(e) = socket.write_all(&buf).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
        });
    }
}