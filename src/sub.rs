#![crate_name = "sub"]

const BROKER_ENDPOINT: &str = "tcp://localhost:5557";
const REQ_RETRIES: u8 = 3;
const REQ_TIMEOUT: i64 = 2500;

use rand::prelude::*;
use std::env;
use uuid::Uuid;

#[derive(Debug)]
enum SubError {
    InvalidArgCount,
    InvalidMethodType,
    EmptyReply,
    CorruptedReply,
    InvalidNonce,
    UnreachableBroker,
}

fn main() -> Result<(), SubError> {

    let id = Uuid::new_v4();

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
            "subscribe" => subscribe(topic, id.to_string()),
            "unsubscribe" => unsubscribe(topic, id.to_string()),
            "get" => get(topic, id.to_string()),
            _ => return Err(SubError::InvalidMethodType),
        },
        None => return Err(SubError::InvalidArgCount),
    }
}

// TODO: Subscribe with a UUID
fn subscribe(topic: String, uuid: String) -> Result<(), SubError> {
    println!("Subscribing to topic {} as uuid {}", topic, uuid);
    send_message(String::from("subscribe"), topic, uuid)
}

// TODO: Unsubscribe using a UUID
fn unsubscribe(topic: String, uuid: String) -> Result<(), SubError> {
    println!("Unsubscribing from topic {} as uuid {}", topic, uuid);
    send_message(String::from("unsubscribe"), topic, uuid)
}
// TODO: Make get retry until it gets the response
fn get(topic: String, uuid: String) -> Result<(), SubError> {
    println!("Getting from topic {}", topic);
    send_message(String::from("get"), topic, uuid)
}

fn send_message(method: String, topic: String, uuid: String) -> Result<(), SubError> {
    let ctx = zmq::Context::new();
    let nonce: u64 = rand::thread_rng().gen();
    let mut retries = REQ_RETRIES;

    while retries > 0 {
        let subscriber = ctx.socket(zmq::REQ).unwrap();
        subscriber.set_linger(1).unwrap();
        assert!(subscriber.connect(BROKER_ENDPOINT).is_ok());
        println!("Subscriber is connected to the broker");

        assert!(subscriber
            .send_multipart(&[uuid.to_owned(), method.to_owned(), topic.to_owned(), nonce.to_string()], 0)
            .is_ok());

        println!(
            "{} Sent message {} to topic {} with nonce {}",
            uuid, method, topic, nonce
        );

        let mut items = [subscriber.as_poll_item(zmq::POLLIN)];
        zmq::poll(&mut items, REQ_TIMEOUT).unwrap();

        let mut reply = zmq::Message::new();
        if items[0].is_readable() && subscriber.recv(&mut reply, 0).is_ok() {
            let recv_reply = match reply.as_str() {
                Some(rep) => rep,
                None => return Err(SubError::EmptyReply),
            };

            let recv_nonce = match recv_reply.parse::<u64>() {
                Ok(seq) => seq,
                Err(_) => return Err(SubError::CorruptedReply),
            };

            if nonce != recv_nonce {
                return Err(SubError::InvalidNonce);
            } else {
                println!("Message delivered to broker");
                break;
            }
        } else {
            println!("Disconnecting from broker");
            assert!(subscriber.disconnect(BROKER_ENDPOINT).is_ok());
            retries -= 1;

            if retries == 0 {
                return Err(SubError::UnreachableBroker);
            } else {
                println!("Could not reach broker. Retrying connection");
            }
        }
    }

    // TODO: Make Ok return with the reply message
    Ok(())
}
