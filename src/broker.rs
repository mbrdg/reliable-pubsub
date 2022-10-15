#![crate_name = "broker"]

// Broker's TODO:
// - [ ] Handling Errors instead of panicking (WIP)
// - [ ] Define how we will connect to the ENDPOINT
// - [ ] Define the protocol with the other parts
//  - [X] Define how the ack will work here in the pub
// - [ ] Define how the service will be instantiated
// - [ ] Define how to dump the current state of the Broker

use std::collections::HashMap;


type Topic = String;
type Message = String;
type Subscriber = String;

const PUT_MESSAGE_TOPIC: usize = 0;
const PUT_MESSAGE_BODY: usize = 1;
const PUT_MESSAGE_NONCE: usize = 2;
const PUT_MESSAGE_FRAMES: usize = 3;

#[derive(Debug, PartialEq, Eq, Hash)]
struct UndeliveredMessage {
    message: Message,
    subscribers: Vec<Subscriber>,
}

fn main() {
    let ctx = zmq::Context::new();

    let backend = ctx.socket(zmq::REP).unwrap();
    let frontend = ctx.socket(zmq::REP).unwrap();

    assert!(backend.bind("tcp://*:5556").is_ok());
    assert!(frontend.bind("tcp://*:5557").is_ok());

    let mut subscribers: HashMap<Topic, Vec<Subscriber>> = HashMap::new();
    let mut messages: HashMap<Topic, Vec<UndeliveredMessage>> = HashMap::new();

    let mut items = [
        backend.as_poll_item(zmq::POLLIN),
        frontend.as_poll_item(zmq::POLLIN),
    ];

    loop {
        zmq::poll(&mut items, -1).unwrap();
        
        if items[0].is_readable() {
            let frames = backend.recv_multipart(0).unwrap();
            assert_eq!(frames.len(), PUT_MESSAGE_FRAMES);

            let topic = String::from_utf8(frames[PUT_MESSAGE_TOPIC].to_owned()).unwrap();
            let body = String::from_utf8(frames[PUT_MESSAGE_BODY].to_owned()).unwrap();
            let nonce = String::from_utf8(frames[PUT_MESSAGE_NONCE].to_owned()).unwrap();

            let ack = zmq::Message::from(&nonce);

            // If there are not any subscribers to a given topic acknowleges immeadiatly
            if !subscribers.contains_key(&topic) {
                assert!(backend.send(ack, 0).is_ok());
                continue;
            }
            
            // Get a copy of the vector of subscribers
            let subs = match subscribers.get(&topic) {
                Some(s) => s.to_vec(),
                None => unreachable!("There are no subscribers to this topic"),
            };

            let undelivered = UndeliveredMessage {
                message: body,
                subscribers: subs,
            };
            
            match messages.get_mut(&topic) {
                Some(v) => v.push(undelivered),
                None => match messages.insert(topic, vec![undelivered]) {
                    None => assert!(backend.send(ack, 0).is_ok()),
                    Some(_) => unreachable!("There are must be no to deliver when creating a new topic"),
                }
            };
        }

        let mut subscriber = zmq::Message::new();
        if items[1].is_readable() && frontend.recv(&mut subscriber, 0).is_ok() {
            // Handle the (un)sub/get messages
        }

        messages.retain(|_, undelivered| !undelivered.is_empty());
        subscribers.retain(|_, subs| !subs.is_empty());
    }
}
