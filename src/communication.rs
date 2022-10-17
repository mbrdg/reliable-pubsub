use crate::errors::PubSubError;
use rand::prelude::*;

// TODO: Add message so that pub works, but make it so that sub doesn't fail

const REQ_RETRIES: u8 = 3;
const REQ_TIMEOUT: i64 = 2500;
const BROKER_ENDPOINT: &str = "tcp://localhost:5557";

pub fn send_message(
    method: String,
    topic: String,
    message: String,
    uuid: String,
) -> Result<(), PubSubError> {
    let ctx = zmq::Context::new();
    let nonce: u64 = rand::thread_rng().gen();
    let mut retries = REQ_RETRIES;

    while retries > 0 {
        let subscriber = ctx.socket(zmq::REQ).unwrap();
        subscriber.set_linger(1).unwrap();
        assert!(subscriber.connect(BROKER_ENDPOINT).is_ok());
        println!("Subscriber is connected to the broker");

        if message.is_empty() {
            assert!(subscriber
                .send_multipart(
                    &[
                        uuid.to_owned(),
                        method.to_owned(),
                        topic.to_owned(),
                        nonce.to_string()
                    ],
                    0
                )
                .is_ok());
        } else {
            assert!(subscriber
                .send_multipart(
                    &[
                        uuid.to_owned(),
                        method.to_owned(),
                        topic.to_owned(),
                        message.to_owned(),
                        nonce.to_string()
                    ],
                    0
                )
                .is_ok());
        }

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
                None => return Err(PubSubError::EmptyReply),
            };

            let recv_nonce = match recv_reply.parse::<u64>() {
                Ok(seq) => seq,
                Err(_) => return Err(PubSubError::MalformedReply),
            };

            if nonce != recv_nonce {
                return Err(PubSubError::InvalidNonce);
            } else {
                println!("Message delivered to broker");
                break;
            }
        } else {
            println!("Disconnecting from broker");
            assert!(subscriber.disconnect(BROKER_ENDPOINT).is_ok());
            retries -= 1;

            if retries == 0 {
                return Err(PubSubError::UnreachableBroker);
            } else {
                println!("Could not reach broker. Retrying connection");
            }
        }
    }

    // TODO: Make Ok return with the reply message
    Ok(())
}
