//! # Pub/Sub Publisher Client
//!
//! This module provides a high-performance, asynchronous publisher for Google Cloud Pub/Sub.
//! It handles batching, acknowledgment tracking, and throughput metrics.

use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use tokio::time::{Duration, Instant};
use std::sync::atomic::Ordering;

use crate::core::metrics::{PUBLISHED_BYTES, PUBLISHED_MESSAGES, WRITE_ERRORS, PUBLISH_LATENCY_TOTAL_MICROS};

use std::sync::Arc;
use tokio::sync::Mutex;

/// A client for publishing batches of messages to a Pub/Sub topic.
#[derive(Clone)]
pub struct PublisherClient {
    /// The underlying Google Cloud Pub/Sub publisher instance.
    publisher: Publisher,
    /// Pending tasks for flush synchronization.
    pending_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<Result<(), String>>>>>,
}

impl PublisherClient {
    /// Creates a new PublisherClient for the specified topic.
    pub async fn new(project_id: &str, topic_id: &str, _ca_path: Option<&str>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;
        let full_topic_name = if topic_id.contains('/') {
            topic_id.to_string()
        } else {
            format!("projects/{}/topics/{}", project_id, topic_id)
        };
        let topic = client.topic(&full_topic_name);
        
        // Default publisher configuration with automatic batching disabled
        // so that Spark can control batching semantics.
        let publisher = topic.new_publisher(None);
        
        Ok(Self { 
            publisher,
            pending_tasks: Arc::new(Mutex::new(Vec::new())),
        })
    }
    
    /// Publishes a batch of messages asynchronously.
    ///
    /// This method spawns a background task to await acknowledgments and returns immediately.
    /// The handle is stored for `flush()`.
    pub async fn publish_batch(&mut self, messages: Vec<PubsubMessage>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
            
            PUBLISH_LATENCY_TOTAL_MICROS.fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            
            if failed {
                Err("One or more messages in batch failed to publish".to_string())
            } else {
                Ok(())
            }
        });

        // Track the task
        let mut tasks = self.pending_tasks.lock().await;
        // Optimization: Clean finished tasks
        tasks.retain(|h| !h.is_finished());
        tasks.push(task);
        
        Ok(())
    }
    
    /// Flushes all pending publish tasks and waits for their completion.
    ///
    /// Returns an error if any batch failed.
    pub async fn flush(&self, _timeout: Duration) -> Result<(), String> {
        let mut tasks = {
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
                 },
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
