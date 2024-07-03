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

  tracing::info!("starting producer");
  producer.start().await.unwrap();
}
```

### ReplicationOp
A record published by the producer is called a `ReplicationOp`, which has the following structure. The published message is JSON encoded using `serde` and `serde_json`. 

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
