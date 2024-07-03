use std::time::Duration;

use async_trait::async_trait;
use consumer::{Consumer, IdempotentApplication};
use producer::{error::ReplicationError, ReplicationOp};
use sqlx::{Postgres, Transaction};
use tracing_subscriber::EnvFilter;

struct LoggerApp {}

#[async_trait]
impl IdempotentApplication for LoggerApp {
    async fn handle_message(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        op: &ReplicationOp,
    ) -> Result<(), ReplicationError> {
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
    let app = LoggerApp {};
    let consumer = Consumer::new("oscar_2", vec!["inventory_2"], "localhost:9092", app);

    tracing::info!("starting idempotent consumer");
    consumer
        .start_idempotent("postgresql://kanko:kanko@localhost:5432/kanko")
        .await
        .unwrap();
}
