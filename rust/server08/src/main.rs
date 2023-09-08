use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use bit_reverse::LookupReverse;

#[derive(Debug)]
enum CipherOperation {
    ReverseBits,
    Xor(u8),
    XorPos,
    Add(u8),
    AddPos,
}

#[derive(Debug)]
enum Operation {
    Encode,
    Decode
}

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    // Bind the TcpListener to the address.
    let listener = TcpListener::bind("0.0.0.0:40000").await?;

    println!("Server running on {:?}", listener.local_addr()?);

    loop {
        // Asynchronously wait for an incoming connection.
        let (socket, addr) = listener.accept().await?;
        let (read_half, mut writer) = socket.into_split();
        let mut reader = BufReader::new(read_half);
        println!("New connection from {:?}", addr);

        // Spawn a new tokio task to process the connection.
        tokio::spawn(async move {
            let mut buf = vec![0u8; 5001];
            let spec: Vec<CipherOperation>;
            let mut read_pos: u32 = 0;
            let mut send_pos: u32 = 0;
            let mut cipher_bytes: Vec<u8> = vec![];
            while let Ok(byte) = reader.read_u8().await {
                match byte {
                    0 => {
                        cipher_bytes.push(0);
                        break
                    }
                    _ => {cipher_bytes.push(byte)}
                }
            }
            // Echo the data.
            // let hex_str = hex::encode(&cipher_bytes);
            // println!("Received in hex: {}, {:?}", hex_str, cipher_bytes);
            // rec_pos = n as u8;
            spec = parse_cipher_spec(&cipher_bytes);
            println!("{} spec: {:?}",addr, spec);
            let test_case = vec![1, 2, 3, 4, 5];
            let encoded_test = translate_stream(test_case.clone(), &spec, 0, Operation::Encode);
            if test_case == encoded_test {
                eprintln!("Invalid Spec");
                return
            }

            let mut dataset: String = String::new();
            // In a loop, read data from the socket and write the data back.
            loop {
                match reader.read(&mut buf).await {
                    // Return value of Ok(0) indicates that the remote has closed.
                    Ok(0) => return,
                    Ok(n) => {
                        let decoded = translate_stream(buf[0..n].to_vec(), &spec, read_pos, Operation::Decode);
                        read_pos += n as u32;

                        dataset = dataset + std::str::from_utf8(&*decoded).unwrap();

                        while dataset.contains('\n') {
                            // println!("{} dataset: {:?}", addr, dataset);
                            let working_set = dataset.clone();
                            let batch_size = working_set.clone().find('\n').unwrap();
                            let (batch, remainder) = working_set.split_at(batch_size);
                            let parts: Vec<&str> = batch.trim().split(',').collect();
                            println!("{} batch: {:?}", addr, dataset);
                            dataset = remainder[1..].to_string();

                            let mut largest = 0;
                            let mut response: &str = "";
                            for part in parts {
                                let quantity = part.splitn(2, "x").next().unwrap().parse::<u32>().unwrap();
                                if quantity > largest {
                                    largest = quantity;
                                    response = part;
                                }
                            }

                            let response = response.to_owned() + "\n";
                            let encoded = translate_stream(Vec::from(response.clone()), &spec, send_pos, Operation::Encode);
                            send_pos = send_pos.wrapping_add(encoded.len() as u32);
                            if let Err(e) = writer.write_all(&*encoded).await {
                                eprintln!("Failed to write to socket: {}", e);
                                return;
                            } else {
                                println!("{} Response sent: {}", addr, response)
                            }
                        }

                    }
                    Err(e) => {
                        eprintln!("Failed to read from socket: {}", e);
                        return;
                    }
                }
            }
        });
    }
}

fn translate_stream(data: Vec<u8>, spec: &Vec<CipherOperation>, mut pos: u32, operation: Operation) -> Vec<u8> {
    let mut result_length = 0;
    let mut iter = data.iter();
    let mut result = vec![0u8; 5000];
    while let Some(byte) = iter.next() {
        let mut current_byte = *byte;
        result_length += 1;

        let specs: Box<dyn Iterator<Item=&CipherOperation>> = match operation {
            Operation::Encode => Box::new(spec.iter()),
            Operation::Decode => Box::new(spec.iter().rev())
        };
        for op in specs {
            match op {
                CipherOperation::ReverseBits => {
                    current_byte = current_byte.swap_bits();
                },
                CipherOperation::Xor(n) => {
                    current_byte = current_byte ^ n;
                },
                CipherOperation::XorPos => {
                    current_byte = current_byte ^ (pos % 256) as u8;
                },
                CipherOperation::Add(n) => {
                    match operation {
                        Operation::Encode => {current_byte = current_byte.wrapping_add(*n)}
                        Operation::Decode => {current_byte = current_byte.wrapping_sub(*n)}
                    }
                },
                CipherOperation::AddPos => {
                    match operation {
                        Operation::Encode => {current_byte = current_byte.wrapping_add((pos % 256) as u8)}
                        Operation::Decode => {current_byte = current_byte.wrapping_sub((pos % 256) as u8)}
                    }
                }
            }
            // println!("{:02x} {}", current_byte, current_byte);

        }
        pos = pos.wrapping_add(1);
        // println!();
        result.push(current_byte)
    }
    result[result.len()-result_length..].to_vec()
}

fn parse_cipher_spec(data: &[u8]) -> Vec<CipherOperation> {
    let mut operations = Vec::new();
    let mut iter = data.iter().cloned();

    while let Some(byte) = iter.next() {
        match byte {
            0 => break,  // End of cipher spec
            1 => operations.push(CipherOperation::ReverseBits),
            2 => operations.push(CipherOperation::Xor(iter.next().unwrap())),
            3 => operations.push(CipherOperation::XorPos),
            4 => operations.push(CipherOperation::Add(iter.next().unwrap())),
            5 => operations.push(CipherOperation::AddPos),
            _ => {
                // Handle invalid cipher spec error
                eprintln!("Invalid cipher spec operation: {}", byte);
                break;
            }
        }
    }

    operations
}
