use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::time::{Instant};
use std::net::UdpSocket;
use itertools::Itertools;


pub(crate) struct Session {
    session_id: u32,
    addr: SocketAddr,
    messages: HashMap<u32, Vec<u8>>,
    closed: bool,
    last_message_time: Instant,
    data_acked: u32,
    data_sent: u32,
    data: Vec<u8>,
    sock: UdpSocket,
}

impl Session {
    pub fn new(session_id: u32, addr: SocketAddr) -> Self {
        Self {
            session_id,
            addr,
            messages: HashMap::new(),
            closed: false,
            last_message_time: Instant::now(),
            data_acked: 0,
            data_sent: 0,
            data: Vec::new(),
            sock: UdpSocket::bind("0.0.0.0:0").expect("Failed to bind sender"),
        }
    }
    pub fn send_ack(&self) {
        let session_id = self.session_id;
        let data_acked = self.data_acked;
        let data = format!("/ack/{session_id}/{data_acked}/");
        self.send(data)
    }

    pub fn ack(&mut self, ord: u32) {
        if ord > self.data_sent {
            self.close()
        } else {
            self.data_acked = ord
        }
    }

    pub fn add_data(&mut self, ord: u32, message: Vec<u8>) {
        self.messages.insert(ord, message);
        let mut keys_to_remove: Vec<u32> = vec![];
        for key in self.messages.keys().sorted() {
            let key= *key as usize;
            if key <= self.data.len(){
                let message = self.messages.get(&(key as u32)).unwrap();
                let message_end = key+message.len();
                if self.data.len() < message_end {
                    self.data.resize(message_end, 0)
                }
                self.data.splice(key..message_end, message.to_owned());
                keys_to_remove.push(key as u32);
            }
        }
        for key in keys_to_remove{
            self.messages.remove(&key);
        }

    }


    pub fn close(&mut self) {
        self.closed = true;
        let session_id = self.session_id;
        let message = format!("/close/{session_id}/");
        self.send(message)
    }

    fn send(&self, message: String) {
        println!("Sent Message: {:?}, {:?}", message, self.addr);
        let _ = self.sock.send_to(message.as_bytes(), self.addr).unwrap();
    }

}