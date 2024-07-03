use async_trait::async_trait;
use consumer::{BasicApplication, Consumer, ConsumerError};
use producer::ReplicationOp;

struct BasicApp {}

#[async_trait]
impl BasicApplication for BasicApp {
    async fn handle_message(&self, _op: &ReplicationOp) -> Result<(), ConsumerError> {
        // do some processing here

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    start_basic_consumer().await
}

async fn start_basic_consumer() {
    let app = BasicApp {};
    let consumer = Consumer::new("alpha", vec!["inventory"], "localhost:9092", app);

    tracing::info!("starting basic consumer");
    consumer.start_basic().await.unwrap();
}
