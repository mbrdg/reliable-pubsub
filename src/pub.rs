#![crate_name = "pub"]

use rand::prelude::*;
use std::env;

#[derive(Debug)]
enum PubError {
    InvalidArgCount,
    EmptyReply,
    MalformedReply,
    InvalidACK,
    UnreachableBroker,
}

const REQ_TIMEOUT: i64 = 2500;
const REQ_RETRIES: i32 = 3;
const BROKER: &str = "tcp://localhost:5556";

fn main() -> Result<(), PubError> {

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: pub <topic> <message>");
        return Err(PubError::InvalidArgCount);
    }

    let mut rng = rand::thread_rng();
    let ctx = zmq::Context::new();

    let topic = &args[1];
    let message = &args[2];
    let nonce: u64 = rng.gen();

    let mut retries_left = REQ_RETRIES;

    while retries_left > 0 {
        let publisher = ctx.socket(zmq::REQ).unwrap();
        publisher.set_linger(0).unwrap();
        assert!(publisher.connect(BROKER).is_ok());
        println!("Info: Connecting to the Broker");

        assert!(publisher.send_multipart(&[topic, message, &nonce.to_string()], 0).is_ok());

        let mut items = [
            publisher.as_poll_item(zmq::POLLIN),
        ];

        zmq::poll(&mut items, REQ_TIMEOUT).unwrap();

        let mut reply = zmq::Message::new();
        if items[0].is_readable() && publisher.recv(&mut reply, 0).is_ok() {

            let recv_reply = match reply.as_str() {
                Some(rep) => rep,
                None => return Err(PubError::EmptyReply),
            };
            let recv_nonce = match recv_reply.parse::<u64>() {
                Ok(seq) => seq,
                Err(_) => return Err(PubError::MalformedReply),
            };

            if nonce != recv_nonce {
                return Err(PubError::InvalidACK);
            } else {
                println!("Info: Message delivered to the Broker");
                break;
            }
        } else {
            assert!(publisher.disconnect(BROKER).is_ok());
            retries_left -= 1;

            if retries_left == 0 {
                return Err(PubError::UnreachableBroker);
            } else {
                println!("Warn: No response from the Broker, retrying...");
            }
        }
    }

    Ok(())
}
