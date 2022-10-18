#![crate_name = "broker"]

// Broker's TODO:
// - [ ] Define how to dump the current state of the Broker

use std::collections::HashMap;


type Topic = String;
type Message = String;
type Subscriber = String;


#[derive(Debug, PartialEq, Eq, Hash)]
struct UndeliveredMessage {
    nonce: u64,
    body: Message,
    subscribers: Vec<Subscriber>,
}

fn main() {
    let ctx = zmq::Context::new();

    let backend = ctx.socket(zmq::REP).unwrap();
    let frontend = ctx.socket(zmq::REP).unwrap();
    let gc = ctx.socket(zmq::REP).unwrap();

    assert!(backend.bind("tcp://*:5556").is_ok());
    assert!(frontend.bind("tcp://*:5557").is_ok());
    assert!(gc.bind("tcp://*:5558").is_ok());

    let mut subscribers: HashMap<Topic, Vec<Subscriber>> = HashMap::new();
    let mut messages: HashMap<Topic, Vec<UndeliveredMessage>> = HashMap::new();

    let mut items = [
        backend.as_poll_item(zmq::POLLIN),
        frontend.as_poll_item(zmq::POLLIN),
        gc.as_poll_item(zmq::POLLIN),
    ];

    loop {
        zmq::poll(&mut items, -1).unwrap();

        if items[0].is_readable() {
            let frames = backend.recv_multipart(0).unwrap();
            assert_eq!(frames.len(), 3);

            let topic = String::from_utf8(frames[0].to_owned()).unwrap();
            let body = String::from_utf8(frames[1].to_owned()).unwrap();
            let nonce = String::from_utf8(frames[2].to_owned()).unwrap();

            let ack = zmq::Message::from(&nonce);

            if !subscribers.contains_key(&topic) {
                assert!(backend.send(ack, 0).is_ok());
                continue;
            }

            // Get a copy of the vector of subscribers
            let subs = match subscribers.get(&topic) {
                Some(s) => s.to_vec(),
                None => unreachable!("There are no subscribers to this topic"),
            };

            let nonce = nonce.parse().unwrap();
            let undelivered = UndeliveredMessage {
                nonce,
                body,
                subscribers: subs,
            };

            match messages.get_mut(&topic) {
                Some(v) => v.push(undelivered),
                None => match messages.insert(topic, vec![undelivered]) {
                    None => assert!(backend.send(ack, 0).is_ok()),
                    Some(_) => unreachable!("Creation of a topic retrieved a message"),
                }
            };
        }

        if items[1].is_readable() {
            let frames = frontend.recv_multipart(0).unwrap();
            assert_eq!(frames.len(), 3);

            let id = String::from_utf8(frames[0].to_owned()).unwrap();
            let operation = std::str::from_utf8(&frames[1][..]).unwrap();
            let topic = String::from_utf8(frames[2].to_owned()).unwrap();

            match operation {
                "subscribe" => {

                    let ack = zmq::Message::from(&id);

                    subscribers
                        .entry(topic)
                        .and_modify(|subs| subs.push(id.to_owned()))
                        .or_insert_with(|| vec![id]);

                    assert!(frontend.send(ack, 0).is_ok());
                },

                "unsubscribe" => {

                    let ack = zmq::Message::from(&id);

                    if let Some(s) = subscribers.get_mut(&topic) {
                        if let Some(i) = s.iter().position(|s| *s == id) {
                            s.remove(i);
                        }
                    }

                    for m in messages.values_mut() {
                        for msg in m {
                            msg.subscribers.retain(|sub| *sub != id);
                        }
                    }

                    assert!(frontend.send(ack, 0).is_ok());
                },

                "get" => {

                    if !subscribers.contains_key(&topic) {
                        assert!(frontend.send_multipart(&["-1", "Topic does not exist"], 0).is_ok());
                        continue;
                    }

                    let subs = match subscribers.get(&topic) {
                        Some(s) => s,
                        None => {
                            assert!(frontend.send_multipart(&["-1", "Topic does not exist"], 0).is_ok());
                            continue;
                        },
                    };

                    if !subs.contains(&id) {
                        assert!(frontend.send_multipart(&["-1", "Not subscribed"], 0).is_ok());
                        continue;
                    }

                    let undelivered = match messages.get(&topic) {
                        Some(u) => u,
                        None => {
                            assert!(frontend.send_multipart(&["-1", "No new messages"], 0).is_ok());
                            continue;
                        },
                    };

                    let next = match undelivered.first() {
                        Some(m) => m,
                        None => {
                            assert!(frontend.send_multipart(&["-1", "No new messages"], 0).is_ok());
                            continue;
                        },
                    };

                    assert!(frontend.send_multipart(&[&next.nonce.to_string(), &next.body], 0).is_ok());
                },
                _ => assert!(frontend.send_multipart(&["-1", "Unkown Operation"], 0).is_ok()),
            }
        }

        if items[2].is_readable() {
            let frames = gc.recv_multipart(0).unwrap();
            assert_eq!(frames.len(), 3);

            let id = String::from_utf8(frames[0].to_owned()).unwrap();
            let topic = String::from_utf8(frames[1].to_owned()).unwrap();
            let _nonce = String::from_utf8(frames[2].to_owned()).unwrap()
                .parse::<u64>().unwrap();

            let undelivered = match messages.get_mut(&topic) {
                Some(v) => v,
                None => {
                    let abort = zmq::Message::from("-1");
                    assert!(gc.send(abort, 0).is_ok());
                    continue;
                },
            };

            let s = match undelivered.first_mut() {
                Some(UndeliveredMessage { nonce: _nonce, body: _, subscribers: s }) => s,
                _ => unreachable!("Broker state must not have change beetween get and gc"),
            };

            if let Some(i) = s.iter().position(|s| *s == id) {
                s.remove(i);
            };

            let ack = zmq::Message::from(&id);
            assert!(gc.send(ack, 0).is_ok());
        }

        messages.retain(|_, undelivered| !undelivered.is_empty());
        subscribers.retain(|_, subs| !subs.is_empty());
    }
}
