use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use async_trait::async_trait;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    message::BorrowedMessage,
    ClientConfig, ClientContext, Message,
};
use serde::Deserialize;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot::{self, Receiver as OneshotReceiver, Sender as OneshotSender},
    RwLock,
};

use crate::error::ConsumerError;
use crate::metrics::Metrics;

pub struct KafkaConsumer<T: Handler> {
    consumer: Arc<StreamConsumer<KafkaConsumerContext<T>>>,
    partition_senders:
        Arc<RwLock<HashMap<(String, i32), (Sender<(T::Payload, i64)>, OneshotSender<()>)>>>,
}

struct KafkaConsumerContext<T: Handler> {
    handler: Arc<T>,
    partition_senders:
        Arc<RwLock<HashMap<(String, i32), (Sender<(T::Payload, i64)>, OneshotSender<()>)>>>,
    offset_tx: Sender<(String, i32, i64)>,
    metrics: Arc<Metrics>,
}

#[async_trait]
pub trait Handler: Sync + Send + 'static {
    type Payload: for<'de> Deserialize<'de> + Send;

    async fn handle_message(
        &self,
        message: &HandlerMessage<'_, Self::Payload>,
    ) -> Result<(), ConsumerError>;

    fn assign_partitions(&self, _: Vec<(&str, i32)>) {}
    fn revoke_partitions(&self, _: Vec<(&str, i32)>) {}
}

pub struct HandlerMessage<'a, T> {
    pub topic: &'a str,
    pub partition: i32,
    pub payload: T,
}

impl<T: Handler> ClientContext for KafkaConsumerContext<T> {}

impl<T: Handler> ConsumerContext for KafkaConsumerContext<T> {
    fn pre_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'a>) {
        match rebalance {
            Rebalance::Assign(assignments) => {
                tracing::info!("assignments received");

                if assignments.capacity() == 0 {
                    return;
                }

                // spawn new tasks to handle the assigned partitions and put the tx channels in partition_senders
                let handler = self.handler.clone();
                let offset_tx = self.offset_tx.clone();
                let partition_senders = self.partition_senders.clone();
                let metrics = self.metrics.clone();

                tokio::task::block_in_place(move || {
                    let mut partition_senders = partition_senders.blocking_write();
                    for ass in assignments.elements().iter() {
                        tracing::info!("+ topic: {}, partition: {}", ass.topic(), ass.partition(),);

                        let (payload_tx, payload_rx) = mpsc::channel(1000000);
                        let (shutdown_tx, shutdown_rx) = oneshot::channel();

                        handle_partition_messages(
                            ass.topic().to_string(),
                            ass.partition(),
                            handler.clone(),
                            payload_rx,
                            shutdown_rx,
                            offset_tx.clone(),
                            metrics.clone(),
                        );
                        partition_senders.insert(
                            (ass.topic().to_string(), ass.partition()),
                            (payload_tx, shutdown_tx),
                        );
                    }
                });

                let ass = assignments.elements();
                let ass = ass
                    .iter()
                    .map(|e| (e.topic(), e.partition()))
                    .collect::<Vec<_>>();

                self.handler.assign_partitions(ass);
            }
            Rebalance::Revoke(revocations) => {
                tracing::info!("revocations received");

                if revocations.capacity() == 0 {
                    return;
                }

                // remove the partition_senders for the revoked partitions
                let partition_senders = self.partition_senders.clone();
                tokio::task::block_in_place(move || {
                    let mut partition_senders = partition_senders.blocking_write();
                    for rev in revocations.elements().iter() {
                        tracing::info!("- topic: {}, partition: {}", rev.topic(), rev.partition());

                        if let Some((_, shutdown_tx)) =
                            partition_senders.remove(&(rev.topic().to_string(), rev.partition()))
                        {
                            // if this fails it means the channel is closed and
                            // there is nothing to clean up, so we don't care
                            let _ = shutdown_tx.send(());
                        }
                    }
                });

                let rev = revocations.elements();
                let rev = rev
                    .iter()
                    .map(|e| (e.topic(), e.partition()))
                    .collect::<Vec<_>>();

                self.handler.revoke_partitions(rev);
            }
            Rebalance::Error(err) => {
                tracing::warn!("kafka rebalancing error: {}", err)
            }
        }
    }
}

impl<T: Handler> KafkaConsumer<T> {
    pub fn new(
        group_id: &str,
        brokers: &str,
        topics: &Vec<String>,
        handler: T,
    ) -> Result<Self, ConsumerError> {
        let handler = Arc::new(handler);
        let partition_senders = Arc::new(RwLock::new(HashMap::new()));
        let (offset_tx, offset_rx) = mpsc::channel(1);

        let metrics = Arc::new(Metrics::new());

        let context = KafkaConsumerContext {
            handler: handler.clone(),
            partition_senders: partition_senders.clone(),
            offset_tx,
            metrics: metrics.clone(),
        };

        let consumer: StreamConsumer<KafkaConsumerContext<T>> = ClientConfig::new()
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            // Commit automatically every 5 seconds
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            // but only commit the offsets explicitly stored via `consumer.store_offset`.
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)?;

        let topics = topics
            .iter()
            .map(|topic| topic.as_str())
            .collect::<Vec<_>>();

        consumer.subscribe(&topics)?;

        let consumer = Arc::new(consumer);

        // spawns a task to listen to offset_rx and update offsets on the consumer
        handle_committed_offsets(consumer.clone(), offset_rx);

        Ok(Self {
            consumer,
            partition_senders,
            // metrics,
        })
    }

    pub async fn consume(self) -> Result<(), ConsumerError> {
        loop {
            tracing::info!("polling for messages");
            match self.consumer.recv().await {
                Ok(msg) => self.route_message(&msg).await?,
                Err(err) => {
                    tracing::error!("Potentially unrecoverable kafka error, will treat it as recoverable unless proven otherwise {}", err);

                    return Err(ConsumerError::Recoverable(anyhow!(
                        "Unexpected Kafka error: {}",
                        err
                    )));
                }
            }
        }
    }

    async fn route_message(&self, msg: &BorrowedMessage<'_>) -> Result<(), ConsumerError> {
        let payload = extract_payload::<T>(msg)?;

        // determine the partition to send the message to
        let partition_senders = self.partition_senders.read().await;
        let (tx, _) = partition_senders.get(&(msg.topic().to_string(), msg.partition())).ok_or_else(|| {
            tracing::warn!("received message for an unassigned partition, will consider this a retryable error");
            ConsumerError::Recoverable(anyhow!("protocol error: received message for an unassigned partition"))
        })?;

        tracing::info!("routing message to appropriate partition handler");

        tx.send((payload, msg.offset())).await.map_err(|err| {
            tracing::error!("received message for a partition for which processing has shutdown due to a downstream error, will consider this a fatal error");
            ConsumerError::Fatal(anyhow!("partition processor failed: {}", err))
        })?;

        Ok(())
    }
}

fn handle_committed_offsets<T: Handler>(
    consumer: Arc<StreamConsumer<KafkaConsumerContext<T>>>,
    mut offset_rx: Receiver<(String, i32, i64)>,
) {
    tokio::spawn(async move {
        while let Some((topic, partition, offset)) = offset_rx.recv().await {
            tracing::info!(
                "updating topic: {}, partition: {} with offset: {}",
                topic,
                partition,
                offset
            );

            if let Err(err) = consumer.store_offset(&topic, partition, offset) {
                // this is an expected error condition during rebalancing, with no bearing on the
                // correctness of the programme. We will simply log this and carry on.
                tracing::warn!("failed to update offset store, will carry on {}", err);
            }
        }
    });
}

fn handle_partition_messages<T: Handler>(
    topic: String,
    partition: i32,
    handler: Arc<T>,
    mut payload_rx: Receiver<(T::Payload, i64)>,
    mut shutdown_rx: OneshotReceiver<()>,
    offset_tx: Sender<(String, i32, i64)>,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        tracing::info!(
            "starting up processing for topic: {}, partition: {}",
            topic,
            partition
        );

        'outer: loop {
            tokio::select! {
                Some((payload, offset)) = payload_rx.recv() => {
                    let message = HandlerMessage {
                        topic: &topic,
                        partition,
                        payload,
                    };

                    loop {
                        match handler.handle_message(&message).await {
                            Ok(()) => {
                                tracing::info!(
                                    "handled message successfully, sending offset {} to offset channel",
                                    offset
                                );

                                // report metrics
                                metrics.processed_event().await;

                                // we don't care about the result of this send because it can only fail if the main task has exited
                                let _ = offset_tx.send((topic.to_string(), partition, offset)).await;

                                break;
                            }
                            Err(ConsumerError::Recoverable(err)) => {
                                tracing::warn!(
                                    "Handler reported a recoverable error: {}, we will simply retry the message after a delay",
                                    err
                                );

                                // this will block the current task
                                tokio::time::sleep(Duration::from_secs(3)).await;
                            }
                            Err(ConsumerError::Fatal(err)) => {
                                // an unrecoverable error has occurred downstream
                                // this implies either there is a bug in the downstream application
                                // or the producer of the message is invalid. Either case warrants a sev_1 investigation
                                tracing::error!("Handler reported an unrecoverable error in processing the message, will not process any more messages from this partition, {}", err);
                                payload_rx.close();

                                break 'outer;
                            }
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("shutting down partition handler due to shutdown signal");
                    break;
                }
            }
        }
    });
}

fn extract_payload<T: Handler>(msg: &BorrowedMessage) -> Result<T::Payload, ConsumerError> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => {
            tracing::info!("received a payload");
            tracing::info!("{}", payload);

            serde_json::from_str(payload).map_err(|_| {
                tracing::error!("Unable to deserialize payload into expected handler payload type");
                ConsumerError::Fatal(anyhow!(
                    "Unable to deserialize payload into expected handler payload type"
                ))
            })
        }
        Some(Err(err)) => {
            tracing::error!(
                "Failed to parse message into intermediate utf8. This is a fatal error: {}",
                err
            );
            return Err(ConsumerError::Fatal(anyhow!("Invalid payload")));
        }
        None => {
            tracing::error!("No payload received. This is a fatal error");
            return Err(ConsumerError::Fatal(anyhow!("Invalid payload")));
        }
    }
}
