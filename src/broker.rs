#![crate_name = "broker"]

use std::collections::HashMap;
use std::fs;
use serde::{Serialize, Deserialize};


type Topic = String;
type Message = String;
type Subscriber = String;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
struct UndeliveredMessage {
    nonce: u64,
    body: Message,
    subscribers: Vec<Subscriber>,
}

fn main() {

    let data: String = fs::read_to_string("subscribers.json").unwrap_or_default();
    let mut subscribers: HashMap<Topic, Vec<Subscriber>> =
        serde_json::from_str(&data).unwrap_or_default();

    let data: String = fs::read_to_string("messages.json").unwrap_or_default();
    let mut messages: HashMap<Topic, Vec<UndeliveredMessage>> =
        serde_json::from_str(&data).unwrap_or_default();

    let ctx = zmq::Context::new();

    let backend = ctx.socket(zmq::REP).unwrap();
    let frontend = ctx.socket(zmq::REP).unwrap();
    let gc = ctx.socket(zmq::REP).unwrap();

    assert!(backend.bind("tcp://*:5556").is_ok());
    assert!(frontend.bind("tcp://*:5557").is_ok());
    assert!(gc.bind("tcp://*:5558").is_ok());

    let mut items = [
        backend.as_poll_item(zmq::POLLIN),
        frontend.as_poll_item(zmq::POLLIN),
        gc.as_poll_item(zmq::POLLIN),
    ];

    loop {
        let mut subscribers_has_changes = false;
        let mut messages_has_changes = false;

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
                Some(v) => {
                    if !v.contains(&undelivered) {
                        v.push(undelivered);
                        messages_has_changes = true;
                    }
                }
                None => match messages.insert(topic, vec![undelivered]) {
                    None => {
                        messages_has_changes = true;
                    },
                    Some(_) => unreachable!("Creation of a topic retrieved a message"),
                }
            };
            assert!(backend.send(ack, 0).is_ok())
        }

        if items[1].is_readable() {
            let frames = frontend.recv_multipart(0).unwrap();
            assert_eq!(frames.len(), 3);

            let id = String::from_utf8(frames[0].to_owned()).unwrap();
            let operation = std::str::from_utf8(&frames[1][..]).unwrap();
            let topic = String::from_utf8(frames[2].to_owned()).unwrap();

            match operation {
                "Subscribe" => {

                    let ack = zmq::Message::from(&id);
                    let topics = subscribers.len();

                    subscribers
                        .entry(topic)
                        .and_modify(|subs|
                            if !subs.contains(&id) {
                                subs.push(id.to_owned());
                                subscribers_has_changes = true;
                            })
                        .or_insert_with(|| vec![id]);

                    subscribers_has_changes = subscribers_has_changes || topics < subscribers.len();

                    assert!(frontend.send(ack, 0).is_ok());
                },

                "Unsubscribe" => {

                    let ack = zmq::Message::from(&id);

                    if let Some(s) = subscribers.get_mut(&topic) {
                        if let Some(i) = s.iter().position(|s| *s == id) {
                            s.remove(i);
                            subscribers_has_changes = true;
                        }
                    }

                    for m in messages.values_mut() {
                        for msg in m {
                            let before_retain_len = msg.subscribers.len();
                            msg.subscribers.retain(|sub| *sub != id);
                            if before_retain_len > msg.subscribers.len() {
                                subscribers_has_changes = true;
                            }
                        }
                    }

                    assert!(frontend.send(ack, 0).is_ok());
                },

                "Get" => {

                    if !subscribers.contains_key(&topic) {
                        assert!(frontend.send_multipart(&[&id, "-1", "Topic does not exist"], 0).is_ok());
                        continue;
                    }

                    let subs = match subscribers.get(&topic) {
                        Some(s) => s,
                        None => {
                            assert!(frontend.send_multipart(&[&id, "-1", "Topic does not exist"], 0).is_ok());
                            continue;
                        },
                    };

                    if !subs.contains(&id) {
                        assert!(frontend.send_multipart(&[&id, "-1", "Not subscribed"], 0).is_ok());
                        continue;
                    }

                    let undelivered = match messages.get(&topic) {
                        Some(u) => u,
                        None => {
                            assert!(frontend.send_multipart(&[&id, "-1", "No new messages"], 0).is_ok());
                            continue;
                        },
                    };

                    let mut has_message_to_send = false;
                    let mut next: &UndeliveredMessage = &UndeliveredMessage {
                        nonce: 0,
                        body: "".to_string(),
                        subscribers: vec![]
                    };

                    for msg in undelivered {
                        if msg.subscribers.contains(&id) {
                            has_message_to_send = true;
                            next = msg;
                            break;
                        }
                    }

                    if !has_message_to_send {
                        assert!(frontend.send_multipart(&[&id, "-1", "No new messages to send"], 0).is_ok());
                        continue;
                    }

                    let n = &next.nonce.to_string()[..];
                    let b = &next.body[..];
                    assert!(frontend.send_multipart(&[&id, n, b], 0).is_ok());
                },
                _ => assert!(frontend.send_multipart(&[&id, "-1", "Unkown Operation"], 0).is_ok()),
            }
        }

        if items[2].is_readable() {
            let frames = gc.recv_multipart(0).unwrap();
            assert_eq!(frames.len(), 3);

            let id = String::from_utf8(frames[0].to_owned()).unwrap();
            let _nonce = String::from_utf8(frames[1].to_owned()).unwrap()
                .parse::<u64>().unwrap();
            let topic = String::from_utf8(frames[2].to_owned()).unwrap();

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
                messages_has_changes = true;
            };

            let ack = zmq::Message::from(&id);
            assert!(gc.send(ack, 0).is_ok());
        }

        let before_retain_len = messages.len();
        messages.retain(|_, undelivered| {
            undelivered.retain(|u| !u.subscribers.is_empty());
            !undelivered.is_empty()
        });

        if before_retain_len > messages.len() {
            messages_has_changes = true;
        }

        let before_retain_len = subscribers.len();
        subscribers.retain(|_, subs| !subs.is_empty());
        if before_retain_len > subscribers.len() {
            subscribers_has_changes = true;
        }

        if subscribers_has_changes {
            serde_json::to_writer(
                &fs::File::create("subscribers.json").unwrap(),
                &subscribers
            ).unwrap();
        }

        if messages_has_changes {
            serde_json::to_writer(
                &fs::File::create("messages.json").unwrap(),
                &messages
            ).unwrap();
        }
    }
}
