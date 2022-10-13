#![crate_name = "pub"]

// Pub's TODO:
// - [ ] Handling Errors instead of panicking
// - [ ] Define how we will connect to the ENDPOINT
// - [ ] Define the protocol with the other parts
//  - [ ] Define how the ack will work here in the pub
// - [ ] Define how the service will be instantiated

use std::env;

#[derive(Debug)]
enum PubError {
    InvalidTopic,
    OffilneBroker,
}

fn main() -> Result<(), PubError> {
    const REQ_TIMEOUT: i64 = 2500;
    const REQ_RETRIES: u8 = 3;
    const ENDPOINT: &str = "tcp://localhost:5556";

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: pub <topic>");
        return Err(PubError::InvalidTopic);
    }

    let ctx = zmq::Context::new();

    let publisher = ctx.socket(zmq::REQ).unwrap();
    assert!(publisher.connect(ENDPOINT).is_ok());

    // Need to know how we will generate sequence numbers...
    // UUIDs, maybeee?
    let mut sequence: u64 = 0;
    let mut retries_left: u8 = REQ_RETRIES;

    while retries_left > 0 {
        let topic = &args[1];
        sequence += 1;

        let request = zmq::Message::from(
            &format!("{topic} {sequence}")
        );
        publisher.send(request, 0).unwrap();

        let mut items = [
            publisher.as_poll_item(zmq::POLLIN)
        ];
        zmq::poll(&mut items, REQ_TIMEOUT).unwrap();

        let mut reply = zmq::Message::new();
        if items[0].is_readable() && publisher.recv(&mut reply, 0).is_ok() {
            let recv_sequence = reply
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();

            if recv_sequence == sequence {
                println!("I: Message delivered to the Broker");
                break;
            }

        } else {
            retries_left -= 1;

            if retries_left > 0 {
                println!("W: No response from the Broker, retrying...");
                publisher.disconnect(ENDPOINT).unwrap();
                println!("I: Reconnecting to the Broker");
                publisher.connect(ENDPOINT).unwrap();
            } else {
                return Err(PubError::OffilneBroker);
            }
        }
    }

    return Ok(());
}
