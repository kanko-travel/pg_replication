use std::time::{Duration, SystemTime};

use tokio::sync::mpsc::{self, Sender};

#[derive(Clone)]
pub struct Metrics {
    event_tx: Sender<Event>,
}

enum Event {
    Processed,
}

impl Metrics {
    pub fn new() -> Self {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let start_time = SystemTime::now();
            let mut total_processed = 0;
            let mut total_processed_last = 0;

            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                tokio::select! {
                    Some(event) = event_rx.recv() => {
                        match event {
                            Event::Processed => {
                                total_processed += 1;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let delta = total_processed - total_processed_last;

                        let time_elasped = SystemTime::now().duration_since(start_time).unwrap().as_secs();
                        let throughput_all_time = total_processed as f64 / time_elasped as f64;
                        let throughput_this_window = delta as f64 / 5.00;

                        tracing::info!("processed {} messages in {} seconds", total_processed, time_elasped);
                        tracing::info!("throughput (all time): {:.2} messages per second", throughput_all_time);
                        tracing::info!("throughput (this window): {:.2} messages per second", throughput_this_window);

                        total_processed_last = total_processed;
                    }
                }
            }
        });

        Self { event_tx }
    }

    pub async fn processed_event(&self) {
        let _ = self.event_tx.send(Event::Processed).await;
    }
}
