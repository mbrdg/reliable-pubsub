#[derive(Debug)]
pub enum PubSubError {
    InvalidArgCount,
    InvalidMethodType,
    EmptyReply,
    MalformedReply,
    InvalidNonce,
    UnreachableBroker,
}
