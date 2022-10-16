#![crate_name = "sub"]

const BROKER_ENDPOINT: &str = "tcp://localhost:5557";

use std::env;

#[derive(Debug)]
enum SubError {
    InvalidArgCount,
    InvalidMethodType,
}

fn main() -> Result<(), SubError> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: sub subscribe|unsubscribe|get <topic>");
        return Err(SubError::InvalidArgCount);
    }

    let topic = match args.last() {
        Some(possible_topic) => String::from(possible_topic),
        None => return Err(SubError::InvalidArgCount),
    };

    match args.get(1) {
        Some(method) => match method.as_str() {
            "subscribe" => subscribe(topic),
            "unsubscribe" => unsubscribe(topic),
            "get" => get(topic),
            _ => return Err(SubError::InvalidMethodType),
        },
        None => return Err(SubError::InvalidArgCount),
    }
}

fn subscribe(topic: String) -> Result<(), SubError> {
    let ctx = zmq::Context::new();
    let subscriber = ctx.socket(zmq::REQ).unwrap();

    assert!(subscriber.connect(BROKER_ENDPOINT).is_ok());

    println!("Subscriber is connected to the broker");
    println!("Subscribing to topic {}", topic);

    return Ok(());
}

fn unsubscribe(topic: String) -> Result<(), SubError> {
    println!("Unsubscribing from topic {}", topic);
    unimplemented!();
}
fn get(topic: String) -> Result<(), SubError> {
    println!("Getting from topic {}", topic);
    unimplemented!();
}
