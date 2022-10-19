# SDLE First Assignment
## Reliable Pub/Sub 🚀

SDLE First Assignment of group T04G14.

Group members:

1. Miguel Rodrigues (up201906042@edu.fe.up.pt)
2. Rita Mendes (up201907877@edu.fe.up.pt)
3. Tiago Rodrigues (up201907021@edu.fe.up.pt)
4. Tiago Silva (up201906045@edu.fe.up.pt)

## Running Instructions

1. Ensure you have [`rustup`](https://www.rust-lang.org/tools/install) installed
    - If not, follow the instruction provided in the link above

2. The service is built of 3 different binaries, each with a specific instantion method:
    - *Publisher*: `cargo run --release --bin pub <topic> <message>`
    - *Broker*: `cargo run --release --bin broker`
    - *Subscriber*: `cargo run --release --bin sub <id> <subscribe | unsubscribe | get> <topic>`
