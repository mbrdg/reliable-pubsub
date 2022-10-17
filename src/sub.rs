#![crate_name = "sub"]

mod communication;
mod errors;
use communication::send_message;
use errors::PubSubError;

use std::env;
use uuid::Uuid;

fn main() -> Result<(), PubSubError> {
    let id = Uuid::new_v4();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: sub subscribe|unsubscribe|get <topic>");
        return Err(PubSubError::InvalidArgCount);
    }

    let topic = match args.last() {
        Some(possible_topic) => String::from(possible_topic),
        None => return Err(PubSubError::InvalidArgCount),
    };

    match args.get(1) {
        Some(method) => match method.as_str() {
            "subscribe" => subscribe(topic, id.to_string()),
            "unsubscribe" => unsubscribe(topic, id.to_string()),
            "get" => get(topic, id.to_string()),
            _ => return Err(PubSubError::InvalidMethodType),
        },
        None => return Err(PubSubError::InvalidArgCount),
    }
}

// TODO: Subscribe with a UUID
fn subscribe(topic: String, uuid: String) -> Result<(), PubSubError> {
    println!("Subscribing to topic {} as uuid {}", topic, uuid);
    send_message(String::from("subscribe"), topic, String::new(), uuid)
}

// TODO: Unsubscribe using a UUID
fn unsubscribe(topic: String, uuid: String) -> Result<(), PubSubError> {
    println!("Unsubscribing from topic {} as uuid {}", topic, uuid);
    send_message(String::from("unsubscribe"), topic, String::new(), uuid)
}
// TODO: Make get retry until it gets the response
fn get(topic: String, uuid: String) -> Result<(), PubSubError> {
    println!("Getting from topic {}", topic);
    send_message(String::from("get"), topic, String::new(), uuid)
}
