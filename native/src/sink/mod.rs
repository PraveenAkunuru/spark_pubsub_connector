//! # Pub/Sub Publisher Client
//!
//! This module provides a high-performance, asynchronous publisher for Google Cloud Pub/Sub.
//! It handles batching, acknowledgment tracking, and throughput metrics.

use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use std::sync::atomic::Ordering;
use tokio::time::{Duration, Instant};

use crate::core::metrics::{
    PUBLISHED_BYTES, PUBLISHED_MESSAGES, PUBLISH_LATENCY_TOTAL_MICROS, WRITE_ERRORS,
};

use std::sync::Arc;
use tokio::sync::Mutex;

type PublishJoinHandle = tokio::task::JoinHandle<Result<(), String>>;

/// A client for publishing batches of messages to a Pub/Sub topic.
#[derive(Clone)]
pub struct PublisherClient {
    /// The underlying Google Cloud Pub/Sub publisher instance.
    publisher: Publisher,
    /// Pending tasks for flush synchronization.
    pending_tasks: Arc<Mutex<Vec<PublishJoinHandle>>>,
}

impl PublisherClient {
    /// Creates a new PublisherClient for the specified topic.
    pub async fn new(
        project_id: &str,
        topic_id: &str,
        _ca_path: Option<&str>,
        batch_size: Option<usize>,
        _batch_bytes: Option<usize>,
        batch_duration_ms: Option<u64>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;
        let full_topic_name = if topic_id.contains('/') {
            topic_id.to_string()
        } else {
            format!("projects/{}/topics/{}", project_id, topic_id)
        };
        let topic = client.topic(&full_topic_name);

        // Configure publisher with batching to improve throughput
        // Defaults: 1000 messages, 9.5MB (safe margin below 10MB), 100ms latency
        let publisher_config = google_cloud_pubsub::publisher::PublisherConfig {
            bundle_size: batch_size.unwrap_or(1000),
            flush_interval: std::time::Duration::from_millis(batch_duration_ms.unwrap_or(100)),
            ..Default::default()
        };

        // bundle_bytes_threshold is not supported in this version of google-cloud-pubsub
        // We must rely on bundle_size (count) and flush_interval.
        // Users should lower bundle_size for large messages.
        // let bytes = batch_bytes.unwrap_or(9_500_000);
        // if bytes > 0 {
        //      publisher_config.bundle_bytes_threshold = bytes;
        // }

        let publisher = topic.new_publisher(Some(publisher_config));

        Ok(Self {
            publisher,
            pending_tasks: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Publishes a batch of messages asynchronously.
    ///
    /// This method spawns a background task to await acknowledgments and returns immediately.
    /// The handle is stored for `flush()`.
    pub async fn publish_batch(
        &mut self,
        messages: Vec<PubsubMessage>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut awaiters = Vec::with_capacity(messages.len());
        let start = Instant::now();

        let msg_count = messages.len() as u64;
        let mut total_bytes = 0;

        for msg in messages {
            total_bytes += msg.data.len() as u64;
            // self.publisher.publish() returns a Future<Awaiter>.
            // We await it (fast) to queue the message and get an Awaiter for the delivery result.
            let awaiter = self.publisher.publish(msg).await;
            awaiters.push(awaiter.get());
        }

        PUBLISHED_BYTES.fetch_add(total_bytes, Ordering::Relaxed);
        PUBLISHED_MESSAGES.fetch_add(msg_count, Ordering::Relaxed);

        // Spawn background task to wait for acks
        let task = tokio::spawn(async move {
            let results = futures::future::join_all(awaiters).await;
            let mut failed = false;

            for res in results {
                if let Err(e) = res {
                    log::error!("Rust: Publish error: {:?}", e);
                    WRITE_ERRORS.fetch_add(1, Ordering::Relaxed);
                    failed = true;
                }
            }

            PUBLISH_LATENCY_TOTAL_MICROS
                .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);

            if failed {
                Err("One or more messages in batch failed to publish".to_string())
            } else {
                Ok(())
            }
        });

        // Check for finished tasks and verify success to prevent accumulation
        // Apply backpressure if too many tasks are pending
        const MAX_PENDING: usize = 5000;

        let mut tasks = loop {
            let mut t = self.pending_tasks.lock().await;
            let mut active = Vec::with_capacity(t.len());
            let mut error = None;

            // Drain all tasks and segregate finished vs active
            for h in t.drain(..) {
                if h.is_finished() {
                    match h.await {
                        Ok(Ok(())) => {} // Success
                        Ok(Err(e)) => error = Some(format!("Background task failed: {}", e)),
                        Err(e) => error = Some(format!("Background task panic: {:?}", e)),
                    }
                } else {
                    active.push(h);
                }
            }

            // Check for errors found during drain
            if let Some(e) = error {
                log::error!("Rust: Previous publish task failed: {}", e);
                eprintln!("Rust: Previous publish task failed: {}", e);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)));
            }

            // Restore active tasks to the vector in the lock
            *t = active;

            // Check limit
            if t.len() < MAX_PENDING {
                break t; // Return the lock guard to the outer scope
            }

            // Limit reached: Release lock and wait
            drop(t);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };

        tasks.push(task);

        Ok(())
    }

    /// Flushes all pending publish tasks and waits for their completion.
    ///
    /// Returns an error if any batch failed.
    pub async fn flush(&self, _timeout: Duration) -> Result<(), String> {
        let tasks = {
            let mut t = self.pending_tasks.lock().await;
            t.drain(..).collect::<Vec<_>>()
        };

        if tasks.is_empty() {
            return Ok(());
        }

        log::info!("Rust: Flushing {} pending publish tasks...", tasks.len());
        let results = futures::future::join_all(tasks).await;

        let mut any_error = None;

        for join_res in results {
            match join_res {
                Ok(task_res) => {
                    if let Err(e) = task_res {
                        any_error = Some(e);
                    }
                }
                Err(e) => {
                    any_error = Some(format!("Task panic/cancelled: {:?}", e));
                }
            }
        }

        if let Some(e) = any_error {
            log::error!("Rust: Flush failed: {}", e);
            return Err(e);
        }

        Ok(())
    }
}
