#![crate_name = "sub"]

use std::{env, fmt};

#[derive(Debug)]
enum SubError {
    InvalidArgCount,
    InvalidOperation,
    InvalidACK,
    InvalidStateBroker,
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
    Trigger(&'a Id, SubOperation, &'a Topic),
    Ack(&'a Nonce),
}

const REQ_RETRIES: u8 = 3;
const REQ_TIMEOUT: i64 = 2500;

const BROKER: &str = "tcp://localhost:5557";
const BROKER_GC: &str = "tcp://localhost:5558";


fn main() -> Result<(), SubError> {

    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
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

    if let Some(nonce) = lazy_pirate(&ctx, SubMessage::Trigger(id, operation, topic)).unwrap() {
        if let SubOperation::Get = operation {
            lazy_pirate(&ctx, SubMessage::Ack(&nonce)).unwrap();
        };
    };

    Ok(())
}


fn check_ack(rep: &Vec<Vec<u8>>, envlp_size: usize, token: &Id) -> Result<String, SubError> {
    assert_eq!(rep.len(), envlp_size, "Malformed Reply");

    let val = String::from_utf8_lossy(&rep[0]);
    if val != *token { Ok(val.to_string()) } else { Err(SubError::InvalidACK) }
}

fn lazy_pirate(ctx: &zmq::Context, msg: SubMessage) -> Result<Option<Nonce>, SubError> {
    let mut retries_left = REQ_RETRIES;

    loop {
        let subscriber = ctx.socket(zmq::REQ).unwrap();
        subscriber.set_linger(0).unwrap();

        // Sends the request
        match msg {
            SubMessage::Trigger(id, op, t) => {
                assert!(subscriber.connect(BROKER).is_ok());
                println!("Info: Connection to the Broker");

                let op_str_repr = format!("{:?}", op);
                assert!(subscriber.send_multipart(&[id, &op_str_repr, t], 0).is_ok());
            },
            SubMessage::Ack(ack) => {
                assert!(subscriber.connect(BROKER_GC).is_ok());
                println!("Info: Connection to the Broker");

                assert!(subscriber.send(&ack, 0).is_ok());
            },
        };

        let mut items = [
            subscriber.as_poll_item(zmq::POLLIN),
        ];

        zmq::poll(&mut items, REQ_TIMEOUT).unwrap();

        if items[0].is_readable() {
            let reply = subscriber.recv_multipart(0).unwrap();

            match msg {
                SubMessage::Trigger(id, op, _t) => {

                    match op {
                        SubOperation::Subscribe | SubOperation::Unsubscribe => {
                            match check_ack(&reply, 1, id) {
                                Ok(_) => return Ok(None),
                                Err(e) => return Err(e),
                            }
                        },

                        SubOperation::Get => {
                            match check_ack(&reply, 2, id) {
                                Ok(nonce) => {
                                    let body = String::from_utf8_lossy(&reply[1]);

                                    if nonce == "-1" {
                                        println!("Error: {}", body);
                                        return Err(SubError::InvalidStateBroker);
                                    }

                                    println!("Message: {}", body);
                                    return Ok(Some(nonce));
                                },
                                Err(e) => return Err(e),
                            }
                        },
                    };
                },

                SubMessage::Ack(ack) => {
                    match check_ack(&reply, 1, ack) {
                        Ok(nonce) => {
                            if nonce == "-1" {
                                println!("Error: Invalid nonce");
                                return Err(SubError::InvalidStateBroker);
                            }
                            return Ok(None);
                        },
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
