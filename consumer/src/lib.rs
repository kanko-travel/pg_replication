mod basic_consumer;
mod db;
mod error;
mod idempotent_consumer;
mod kafka_consumer;
mod metrics;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use basic_consumer::BasicConsumer;
use idempotent_consumer::IdempotentConsumer;
use kafka_consumer::KafkaConsumer;
use producer::ReplicationOp;
use sqlx::{Postgres, Transaction};

pub use error::ConsumerError;

pub struct Consumer<App> {
    pub group_id: String,
    pub topics: Vec<String>,
    pub brokers: String,
    app: Arc<App>,
}

#[async_trait]
pub trait IdempotentApplication: Send + Sync + 'static {
    async fn handle_message(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        _op: &ReplicationOp,
    ) -> Result<(), ConsumerError>;
}

#[async_trait]
pub trait BasicApplication: Send + Sync + 'static {
    async fn handle_message(&self, _op: &ReplicationOp) -> Result<(), ConsumerError>;
}

impl<App> Consumer<App> {
    pub fn new(group_id: &str, topics: Vec<&str>, brokers: &str, app: App) -> Self {
        Self {
            group_id: group_id.into(),
            topics: topics.iter().map(|t| t.to_string()).collect(),
            brokers: brokers.into(),
            app: Arc::new(app),
        }
    }
}

impl<App: IdempotentApplication> Consumer<App> {
    pub async fn start_idempotent(self, connection_string: &str) -> Result<(), anyhow::Error> {
        loop {
            match self.start_idempotent_consumer(connection_string).await {
                Err(ConsumerError::Recoverable(_)) => {
                    tracing::warn!("failed to create idempotent consumer, will retry");
                }
                Err(ConsumerError::Fatal(err)) => {
                    tracing::error!("encountered a fatal error, please investigate: {}", err);
                    return Err(err);
                }
                Ok(_) => unreachable!("consumer cannot exit without an error"),
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
            tracing::info!("resuming consumer for group_id {}", &self.group_id);
        }
    }

    async fn start_idempotent_consumer(
        &self,
        connection_string: &str,
    ) -> Result<(), ConsumerError> {
        let idempotent_consumer =
            IdempotentConsumer::new(&self.group_id, connection_string, self.app.clone()).await?;

        let kafka_consumer = KafkaConsumer::new(
            &self.group_id,
            &self.brokers,
            &self.topics,
            idempotent_consumer,
        )?;
        kafka_consumer.consume().await
    }
}

impl<App: BasicApplication> Consumer<App> {
    pub async fn start_basic(self) -> Result<(), anyhow::Error> {
        loop {
            match self.start_basic_consumer().await {
                Err(ConsumerError::Recoverable(_)) => {
                    tracing::warn!("failed to create idempotent consumer, will retry");
                }
                Err(ConsumerError::Fatal(err)) => {
                    tracing::error!("encountered a fatal error, please investigate: {}", err);
                    return Err(err);
                }
                Ok(_) => unreachable!("consumer cannot exit without an error"),
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
            tracing::info!("resuming consumer for group_id {}", &self.group_id);
        }
    }

    async fn start_basic_consumer(&self) -> Result<(), ConsumerError> {
        let basic_consumer = BasicConsumer::new(self.app.clone());

        let kafka_consumer =
            KafkaConsumer::new(&self.group_id, &self.brokers, &self.topics, basic_consumer)?;
        kafka_consumer.consume().await
    }
}
