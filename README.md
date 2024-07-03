# PG Replication
A lightweight logical replication library for Postgres consisting of `producer` and `consumer` crates. 

## Producer
The producer reads from a designated logical replication slot and writes records for all tables in a given publication to kafka, according to a custom mapping from each table to a kafka topic.
Configuration of the producer further allows defining a set of columns as the partition key when publishing to the topic.
It is guaranteed to produce records in the order in which they appear in the WAL and produces each record at least once, with no gaps in between records.

### Example Producer Config

```rust
#[tokio::main]
fn main() {
  let topic_map = serde_json::json!({
      "inventory_table": {
          "name": "inventory_topic",
          "partition_key": ["organization_id"]
      },
  });

  let topic_map: HashMap<String, TopicInfo> = serde_json::from_value(topic_map).unwrap();

  let producer = Producer::new(
      // a postgres connection string
      "postgresql://username:password@localhost:5432/database".into(),

      // a comma separated list of kafka brokers to connect to
      "localhost:9092".into(),

      // what to name the logical replication slot (this slot will be automatically created)
      "slot_1".into(),

      // the name of the publication (the tables defined in the topic map must be part of the publication)
      "publication_1".into(),

      // the topic map as defined above
      topic_map,
  );

  producer.start().await.unwrap();
}
```

### ReplicationOp
A record published by the producer is called a `ReplicationOp`. The published message is JSON encoded using `serde` and `serde_json`. The structure of `ReplicationOp` is as follows:

```rust
#[derive(Serialize, Deserialize, Debug)]
pub struct ReplicationOp {
    pub table_name: String,
    pub col_names: Vec<String>,
    pub lsn: u64,
    pub seq_id: u64,
    pub op: Op,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Op {
    Insert(Row),
    Update((Row, Row)),
    Delete(Row),
}

pub type Row = Vec<Option<String>>;
```

- `col_names` is a list of column names in the order in which row values appear.
- `lsn` is the log sequence number of the transaction that produced the record.
- `seq_id` is a number indicating the order of the operation within the transaction. It is monotonically increasing and increments by exactly one for each subsequent operation.
- `op` is one of Insert, Update or Delete. It contains a `Row` in the case of an `Insert` or `Delete`, or a tuple of (`Row`, `Row`) representing the old and new rows respectively.
- `Row` is a list of text encoded column values, where each column value occurs in the same position as its name in `col_names`.

## Consumer
The consumer crate exposes an API to create two types of consumers:
1. Basic Consumer
2. Idempotent Consumer

### 1. Basic Consumer
Basic consumer is a naive, stateless consumer that processes each message in order (within the same partition). Any app that implements the `BasicApplication` trait can be used with this consumer.

```rust
#[async_trait]
pub trait BasicApplication: Send + Sync + 'static {
    async fn handle_message(&self, op: &ReplicationOp) -> Result<(), ConsumerError>;
}
```

#### Basic Consumer Example

```rust
struct BasicApp {}

#[async_trait]
impl BasicApplication for BasicApp {
    async fn handle_message(&self, op: &ReplicationOp) -> Result<(), ConsumerError> {
        // do some processing here

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let app = BasicApp {};
    let consumer = Consumer::new("consumer_group_name", vec!["topic_name_1", "topic_name_2"], "localhost:9092", app);

    consumer.start_basic().await.unwrap();
}
```

### 2. Idempotent Consumer
Idempotent consumer is a Kafka consumer that allows processing each `ReplicationOp` exactly once, with guaranteed ordering of processed messages within the same partition. It does so by synchrnonizing state with a Postgres backend, which is assumed to be the same database where the underlying application would store any persistent side-effects from processing the message. It does so by exposing the database transaction in which it does its state synchronization to the underlying application. Any app that implements the `IdempotentApplication` trait can be used with this consumer.

```rust
#[async_trait]
pub trait IdempotentApplication: Send + Sync + 'static {
    async fn handle_message(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        op: &ReplicationOp,
    ) -> Result<(), ConsumerError>;
}
```

#### Idempotent Consumer Example

```rust
struct IdempotentApp {}

#[async_trait]
impl IdempotentApplication for IdempotentApp {
    async fn handle_message(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        op: &ReplicationOp,
    ) -> Result<(), ConsumerError> {
        // do some processing here

        Ok(())
    }
}

async fn main() {
    let app = IdempotentApp {};
    let consumer = Consumer::new("consumer_group_name", vec!["topic_name_1", "topic_name_2"], "localhost:9092", app);

    consumer
        .start_idempotent("postgresql://username:password@localhost:5432/database")
        .await
        .unwrap();
}
```
