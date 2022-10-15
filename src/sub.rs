#![crate_name = "sub"]

const SERVER_ENDPOINT: &str = "tcp://localhost:5556";
use std::env;

#[derive(Debug)]
enum SubError {
    InvalidArgCount,
}

fn main() -> Result<(), SubError> {
    println!("Hello, World");

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: sub subscribe|unsubscribe|get <topic>");
        return Err(SubError::InvalidArgCount);
    }

    let topic = args.last();

    match args.get(1) {
        "subscribe" => subscribe(topic),
        "unsubscribe" => unsubscribe(topic),
        "get" => get(topic),
    }
}

fn subscribe(topic: String) -> Result<(), SubError> {
    let ctx = zmq::Context::new();
    let subscriber = ctx.socket(zmq::REQ).unwrap();
    assert!(subscriber.connect(SERVER_ENDPOINT).is_ok());

    return Err(SubError::InvalidArgCount);
}

fn unsubscribe(topic: String) -> Result<(), SubError> {
    unimplemented!();
}
fn get(topic: String) -> Result<(), SubError> {
    unimplemented!();
}
