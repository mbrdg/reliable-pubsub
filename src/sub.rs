#![crate_name = "sub"]

use std::{env, fmt};
use zmq;

#[derive(Debug)]
enum SubError {
    InvalidArgCount,
    InvalidOperation,
    MalformedReply,
    InvalidACK,
    UnreachableBroker,
}

#[derive(Debug, Clone, Copy)]
enum SubOperation {
    Subscribe,
    Unsubscribe,
    Get,
}

impl fmt::Display for SubOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SubOperation::Subscribe => write!(f, "subscribe"),
            SubOperation::Unsubscribe => write!(f, "unsubscribe"),
            SubOperation::Get => write!(f, "get"),
        }
    }
}

type Id = String;
type Topic = String;
type Nonce = String;

enum SubMessage<'a> {
    Issue(&'a Id, SubOperation, &'a Topic),
    Ack(&'a Nonce),
}

const REQ_RETRIES: u8 = 3;
const REQ_TIMEOUT: i64 = 2500;
const BROKER: &str = "tcp://localhost:5557";

const ACK_LEN: usize = 1;
const GET_ACK_LEN: usize = 2;

fn main() -> Result<(), SubError> {

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: sub <id> <subscribe | unsubscribe | get> <topic>");
        return Err(SubError::InvalidArgCount);
    }

    let ctx = zmq::Context::new();

    let id = &args[1];
    let operation: SubOperation = match &args[2][..] {
        "subscribe" => SubOperation::Subscribe,
        "unsubscribe" => SubOperation::Unsubscribe,
        "get" => SubOperation::Get,
        _ => return Err(SubError::InvalidOperation),
    };
    let topic = &args[3];

    if let Some(nonce) = lazy_pirate(&ctx, SubMessage::Issue(id, operation, topic)).unwrap() {
        if let SubOperation::Get = operation {
            lazy_pirate(&ctx, SubMessage::Ack(&nonce)).unwrap();
        };
    };

    Ok(())
}


fn check_ack(rep: &Vec<Vec<u8>>, expected_size: usize, token: &Id) -> Result<String, SubError> {
    if rep.len() != expected_size {
        return Err(SubError::MalformedReply);
    }

    let val = String::from_utf8_lossy(&rep[0]);
    if val != *token { Ok(val.to_string()) } else { Err(SubError::InvalidACK) }
}

fn lazy_pirate(ctx: &zmq::Context, msg: SubMessage) -> Result<Option<Nonce>, SubError> {
    let mut retries_left = REQ_RETRIES;
    loop {
        let subscriber = ctx.socket(zmq::REQ).unwrap();
        subscriber.set_linger(0).unwrap();
        assert!(subscriber.connect(BROKER).is_ok());
        println!("Info: Connection to the Broker");

        match msg {
            SubMessage::Issue(id, op, t) => {
                let op_str_repr = format!("{:?}", op);
                assert!(subscriber.send_multipart(&[id, &op_str_repr, t], 0).is_ok());
            },
            SubMessage::Ack(ack) => {
                assert!(subscriber.send(ack, 0).is_ok());
            },
        };

        let mut items = [
            subscriber.as_poll_item(zmq::POLLIN),
        ];

        zmq::poll(&mut items, REQ_TIMEOUT).unwrap();

        if items[0].is_readable() {

            let reply = subscriber.recv_multipart(0).unwrap();
            match msg {
                SubMessage::Issue(id, op, _t) => {
                    match op {
                        SubOperation::Subscribe | SubOperation::Unsubscribe => {
                            match check_ack(&reply, ACK_LEN, id) {
                                Ok(_) => return Ok(None),
                                Err(e) => return Err(e),
                            }
                        },
                        SubOperation::Get => {
                            match check_ack(&reply, GET_ACK_LEN, id) {
                                Ok(nonce) => {
                                    let body = String::from_utf8_lossy(&reply[1]);
                                    println!("{}", body);
                                    return Ok(Some(nonce));
                                },
                                Err(e) => return Err(e),
                            }
                        },
                    };
                },
                SubMessage::Ack(ack) => {
                    match check_ack(&reply, ACK_LEN, ack) {
                        Ok(_) => return Ok(None),
                        Err(e) => return Err(e),
                    }
                }
            }

        } else {
            assert!(subscriber.disconnect(BROKER).is_ok());
            retries_left -= 1;

            if retries_left == 0 {
                return Err(SubError::UnreachableBroker);
            } else {
                println!("Warn: No response from the Broker, retrying...");
            }
        }
    };
}
