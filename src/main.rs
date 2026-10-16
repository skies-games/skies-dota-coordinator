//! Coordinator binary entrypoint.

use coordinator::runtime;

#[tokio::main]
async fn main() {
    runtime::run().await;
}
