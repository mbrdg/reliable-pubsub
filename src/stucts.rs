#![crate_name = "structs"]

// Hash<String, Vec<u64>> - subscribers

#[derive(Debug, PartialEq, Eq, Hash)]
struct TopcMessage {
    message_id: u64,
    subscribers: Vec<(u64, bool)>,
}

// Hash<String, Vec<TopcMessage>> 

fn main() {
    println!("Hello, world!");
}
