use std::time::Duration;

use async_trait::async_trait;
use consumer::{Consumer, ConsumerError, IdempotentApplication};
use producer::ReplicationOp;
use sqlx::{Postgres, Transaction};
use tracing_subscriber::EnvFilter;

struct IdempotentApp {}

#[async_trait]
impl IdempotentApplication for IdempotentApp {
    async fn handle_message(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        op: &ReplicationOp,
    ) -> Result<(), ConsumerError> {
        tracing::info!("handling message");
        tracing::info!("{:?}", op);

        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let filter = EnvFilter::new("warn,consumer::metrics=info");

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter) // Set the log level to WARN and above
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();

    start_idempotent_consumer().await
}

async fn start_idempotent_consumer() {
    let app = IdempotentApp {};
    let consumer = Consumer::new("oscar_2", vec!["inventory_2"], "localhost:9092", app);

    tracing::info!("starting idempotent consumer");
    consumer
        .start_idempotent("postgresql://kanko:kanko@localhost:5432/kanko")
        .await
        .unwrap();
}
