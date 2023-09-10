use std::cmp::Ordering;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, BinaryHeap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Job {
    id: u32,
    job: serde_json::Value,
    pri: i32,
    queue: String,
}

impl PartialEq<Self> for Job {
    fn eq(&self, other: &Self) -> bool {
        self.pri.eq(&other.pri)
    }
}

impl PartialOrd<Self> for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.pri.partial_cmp(&other.pri)
    }
}

impl Eq for Job {}


impl Ord for Job{
    fn cmp(&self, other: &Self) -> Ordering {
        self.pri.cmp(&other.pri)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Request {
    request: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pri: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queues: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait: Option<bool>,
}



#[derive(Debug, Serialize, Deserialize)]
struct Response {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pri: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:40000").await.unwrap();
    let job_queues = Arc::new(Mutex::new(HashMap::new()));
    let job_id_counter = Arc::new(Mutex::new(0));


    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("Client Connected: {}", addr);
        let job_queues = Arc::clone(&job_queues);
        let job_id_counter = Arc::clone(&job_id_counter);
        tokio::spawn(async move {
            handle_connection(socket, job_queues, job_id_counter, addr).await;
        });
    }
}

async fn handle_connection(stream: TcpStream, job_queues: Arc<Mutex<HashMap<String, BinaryHeap<(i32, Job)>>>>, job_id_counter: Arc<Mutex<u32>>, addr: SocketAddr) {
    let mut buf = vec![0; 2048];
    let job_log:Arc<Mutex<HashMap<u32, Job>>> = Arc::new(Mutex::new(HashMap::new()));
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let new_line = '\n' as u8;
    loop {

        match reader.read_until(new_line, &mut buf).await {
        // match read.peek(&mut buf).await {
        // match read.read(&mut buf).await {
            Ok(0) => {
                println!("{} disconnects ", addr);
                restore_jobs(job_log, job_queues);
                break
            },
            Ok(n) => {
                // println!("raw: {:?}", std::str::from_utf8(&buf[buf.len()-n..]).unwrap());

                let request: Request = serde_json::from_slice(&buf[buf.len()-n..]).unwrap_or_else(|_| Request { request: "invalid".to_string(), queue: None, job: None, pri: None, id: None, queues: None, wait: None });
                // println!("{} request: {:?}", addr, request);
                let response = match request.request.as_str() {
                    "put" => handle_put(request, &job_queues, &job_id_counter),
                    "get" => handle_get(request, &job_queues, &job_log).await,
                    "abort" => handle_abort(request, &job_queues, &job_log),
                    "delete" => handle_delete(request, &job_queues, &job_log),
                    // Handle other request types...
                    _ => Response { status: "error".to_string(), error: Some("Unknown request type.".to_string()), id: None, job: None, pri: None, queue: None }
                };
                // println!("{} response: {:?}", addr, response);

                let response_data = serde_json::to_vec(&response).unwrap();
                write.write_all(&response_data).await.unwrap();
                write.write_all(&[new_line]).await.unwrap();
                write.flush().await.expect("TODO: panic message");
            },
            Err(_) => {
                println!("Client disconnects {}", addr);
                restore_jobs(job_log, job_queues);
                break
            },
        }
    }
}

fn handle_delete(request: Request, job_queues: &Arc<Mutex<HashMap<String, BinaryHeap<(i32, Job)>>>>, job_log: &Arc<Mutex<HashMap<u32, Job>>>) -> Response {
    let id = request.id.unwrap();
    let mut deleted = false;
    {
        let mut job_log = job_log.lock().unwrap();
        if job_log.contains_key(&id) {
            job_log.remove(&id);
            deleted = true;
        }
    }
    {
        let mut queue_name = String::new();
        let mut job_queues = job_queues.lock().unwrap();
        for (name, queue) in job_queues.iter(){
            for (_pri, job) in queue.iter() {
                if job.id == id {
                    queue_name = name.clone();
                    deleted = true;
                    break;
                }
            }
        }
        if queue_name.len() > 0 {
            let queue = job_queues.get_mut(&queue_name).unwrap();
            let mut new_queue: BinaryHeap<(i32, Job)> = BinaryHeap::new();
            for (pri, job) in queue.drain() {
                if job.id != id {
                    new_queue.push((pri, job));
                }
            }
            // job_queues.remove(&queue_name);
            job_queues.insert(queue_name, new_queue);
        }
    }
    let response = match deleted {
        true => { Response { status: "ok".to_string(), id: Some(id), job: None, pri: None, queue: None, error: None }}
        false => { Response { status: "no-job".to_string(), id: Some(id), job: None, pri: None, queue: None, error: None }}
    };
    response
}

fn restore_jobs(job_log: Arc<Mutex<HashMap<u32, Job>>>, job_queues: Arc<Mutex<HashMap<String, BinaryHeap<(i32, Job)>>>>){
    let job_log = job_log.lock().unwrap();
    for (_, job) in job_log.iter(){
        let mut job_queues = job_queues.lock().unwrap();
        let queue = job_queues.get_mut(&job.queue).unwrap();
        queue.push((job.pri, job.clone()))
    }
}


fn handle_abort(request: Request, job_queues: &Arc<Mutex<HashMap<String, BinaryHeap<(i32, Job)>>>>, job_log: &Arc<Mutex<HashMap<u32, Job>>>) -> Response {
    let id = request.id.unwrap();
    let mut job_log = job_log.lock().unwrap();

    match job_log.get(&id) {
        None => {},
        Some(job) => {
            let mut job_queues = job_queues.lock().unwrap();
            let queue = job_queues.get_mut(&job.queue).unwrap();
            queue.push((job.pri, job.clone()));
            job_log.remove(&id);
            return Response { status: "ok".to_string(), id: Some(id), job: None, pri: None, queue: None, error: None }
        }
    }
    Response { status: "no-job".to_string(), id: Some(id), job: None, pri: None, queue: None, error: None }
}

fn handle_put(request: Request, job_queues: &Arc<Mutex<HashMap<String, BinaryHeap<(i32, Job)>>>>, job_id_counter: &Arc<Mutex<u32>>) -> Response {
    // Handle the "put" request...
    // This is a basic outline; fill in the logic based on the provided spec.

    let mut id_counter = job_id_counter.lock().unwrap();
    *id_counter += 1;
    let id = *id_counter;

    // Create and insert the job...
    let job = Job { id, job: request.job.unwrap(), pri: request.pri.unwrap(), queue: request.queue.unwrap() };
    let mut queues = job_queues.lock().unwrap();
    let queue = queues.entry(job.queue.clone()).or_insert_with(BinaryHeap::new);
    queue.push((job.pri, job));


    Response { status: "ok".to_string(), id: Some(id), job: None, pri: None, queue: None, error: None }
}


fn get_highest_priority_queue(request: &Request, job_queues: &Arc<Mutex<HashMap<String, BinaryHeap<(i32, Job)>>>>) -> String{
    let queues = job_queues.lock().unwrap();
    let mut highest_queue: String = String::new();
    let mut highest_priority = 0;
    for queue_name in request.queues.clone().unwrap_or_default() {
        match queues.get(&queue_name) {
            None => {}
            Some(queue) => {
                match queue.peek() {
                    None => {}
                    Some((pri, _job)) => {
                        if pri > &highest_priority {
                            highest_priority = *pri;
                            highest_queue = queue_name;
                        }
                    }
                }
            }
        }
    }
    highest_queue
}

async fn handle_get(request: Request, job_queues: &Arc<Mutex<HashMap<String, BinaryHeap<(i32, Job)>>>>, job_log: &Arc<Mutex<HashMap<u32, Job>>>) -> Response {

    let mut highest_queue = get_highest_priority_queue(&request, job_queues);

    while highest_queue == "" && request.wait == Option::from(true) {

        sleep(Duration::from_secs(1)).await;
        highest_queue = get_highest_priority_queue(&request, job_queues);
    }

    let mut queues = job_queues.lock().unwrap();
    match queues.get_mut(&highest_queue) {
        // Extract the highest-priority job, if any...
        // Return the job in a Response...

        None => {}
        Some(queue) => {
            match queue.pop() {
                None => {}
                Some((_pri, job)) => {
                    // println!("{:?}
                    let mut job_log = job_log.lock().unwrap();
                    job_log.insert(job.id, job.clone());
                    return Response {
                        status: "ok".to_string(),
                        error: None,
                        id: Option::from(job.id),
                        job: Option::from(job.job),
                        pri: Option::from(job.pri),
                        queue: Option::from(highest_queue),
                    }
                }
            };

        }
    }

    Response { status: "no-job".to_string(), id: None, job: None, pri: None, queue: None, error: None }
}

// Implement other request handlers similarly...
