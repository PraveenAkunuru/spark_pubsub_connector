//! Pub/Sub client management and asynchronous ingestion.
//!
//! This module handles the lifecycle of Google Cloud Pub/Sub subscribers, including
//! authentication, stream management, and internal buffering for Spark consumption.

use dashmap::DashMap;
use futures::StreamExt;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_googleapis::pubsub::v1::ReceivedMessage as LowLevelMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscriber::SubscriberConfig;
use google_cloud_pubsub::subscription::SubscribeConfig;
use once_cell::sync::Lazy;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Duration, Instant};

use crate::core::metrics::{BUFFERED_BYTES, INGESTED_BYTES, INGESTED_MESSAGES, READ_ERRORS};
use crate::source::ACK_HANDLE_MAP;

/// Global registry of active Pub/Sub clients, keyed by an integer identifier.
/// This allows JNI calls to reference persistent client state across micro-batches.
pub static CLIENT_REGISTRY: Lazy<DashMap<i32, Arc<PubSubClient>>> = Lazy::new(DashMap::new);

/// A wrapper around the Pub/Sub subscriber client providing buffering and batching.
pub struct PubSubClient {
    /// Internal receiver for messages pulled from the Pub/Sub service.
    receiver: AsyncMutex<mpsc::Receiver<LowLevelMessage>>,
}

impl PubSubClient {
    /// Creates a new PubSubClient and starts a background pull task.
    ///
    /// # Arguments
    /// * `project_id` - GCP Project ID.
    /// * `subscription_id` - Subscription ID or full resource name.
    /// * `_ca_path` - Optional path to CA certificates (reserved for private environments).
    pub async fn new(
        project_id: &str,
        subscription_id: &str,
        _ca_path: Option<&str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        log::info!(
            "Rust: [First Principles] Initializing PubSubClient for {}/{}",
            project_id,
            subscription_id
        );

        // Load default config (with auth)
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;

        let full_sub_name = if subscription_id.contains('/') {
            subscription_id.to_string()
        } else {
            format!("projects/{}/subscriptions/{}", project_id, subscription_id)
        };

        let subscription = client.subscription(&full_sub_name);

        let sub_config = SubscriberConfig {
            max_outstanding_messages: 10_000, // Reduced from 20k to be safer across many partitions
            max_outstanding_bytes: 200 * 1024 * 1024, // 200MB (Reduced from 500MB)
            ..Default::default()
        };

        let config = SubscribeConfig::default().with_subscriber_config(sub_config);

        // Channel to bridge Library -> Spark (Deep buffer)
        let (tx, rx) = mpsc::channel::<LowLevelMessage>(20_000); // Reduced from 50k
        let sub_clone = subscription.clone();
        let sub_name_clone = full_sub_name.clone();

        tokio::spawn(async move {
            log::info!(
                "Rust: High-Level Subscriber task starting for {}",
                sub_name_clone
            );

            let mut stream = match sub_clone.subscribe(Some(config)).await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("Rust: Subscription failed for {}: {:?}", sub_name_clone, e);
                    READ_ERRORS.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            while let Some(msg) = stream.next().await {
                let ack_id = msg.ack_id().to_string();

                let size = msg.message.data.len();
                BUFFERED_BYTES.fetch_add(size, Ordering::Relaxed);
                INGESTED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
                INGESTED_MESSAGES.fetch_add(1, Ordering::Relaxed);

                let m = &msg.message;
                let pubsub_msg = PubsubMessage {
                    data: m.data.clone(),
                    attributes: m.attributes.clone(),
                    message_id: m.message_id.clone(),
                    publish_time: m.publish_time,
                    ordering_key: m.ordering_key.clone(),
                };

                let delivery_attempt = msg.delivery_attempt().unwrap_or(0) as i32;
                let low_level = LowLevelMessage {
                    ack_id: ack_id.clone(),
                    message: Some(pubsub_msg),
                    delivery_attempt,
                };

                // Track handle in global map (via source module)
                ACK_HANDLE_MAP.insert(ack_id.clone(), msg);

                if tx.send(low_level).await.is_err() {
                    log::info!(
                        "Rust: Internal channel full or closed for {}. Stop pulling.",
                        sub_name_clone
                    );
                    break;
                }
            }
            log::info!(
                "Rust: High-Level Subscriber task ended for {}",
                sub_name_clone
            );
        });

        Ok(PubSubClient {
            receiver: AsyncMutex::new(rx),
        })
    }

    /// Fetches a batch of messages from the internal buffer.
    ///
    /// Blocks until `max_messages` are received or `wait_ms` expires.
    pub async fn fetch_batch(
        &self,
        max_messages: usize,
        wait_ms: u64,
    ) -> Result<Vec<LowLevelMessage>, Box<dyn std::error::Error + Send + Sync>> {
        let mut messages = Vec::with_capacity(max_messages);
        let deadline = Instant::now() + Duration::from_millis(wait_ms);

        let mut rx = self.receiver.lock().await;

        while messages.len() < max_messages {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            match tokio::time::timeout(deadline - now, rx.recv()).await {
                Ok(Some(msg)) => {
                    let size = msg.message.as_ref().map(|m| m.data.len()).unwrap_or(0);
                    BUFFERED_BYTES.fetch_sub(size, Ordering::Relaxed);
                    messages.push(msg);

                    while messages.len() < max_messages {
                        match rx.try_recv() {
                            Ok(m) => {
                                let s = m.message.as_ref().map(|x| x.data.len()).unwrap_or(0);
                                BUFFERED_BYTES.fetch_sub(s, Ordering::Relaxed);
                                messages.push(m);
                            }
                            Err(_) => break,
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        Ok(messages)
    }

    /// Acknowledges a list of messages by their AckIds.
    ///
    /// This removes the handles from the global `ACK_HANDLE_MAP` and triggers
    /// the asynchronous acknowledgment call to the Pub/Sub service.
    pub async fn acknowledge(
        &self,
        ack_ids: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if ack_ids.is_empty() {
            return Ok(());
        }

        let mut handles = Vec::new();
        for id in ack_ids {
            if let Some((_, msg)) = ACK_HANDLE_MAP.remove(&id) {
                handles.push(msg);
            }
        }

        if !handles.is_empty() {
            tokio::spawn(async move {
                let ack_futures = handles.into_iter().map(|msg| async move {
                    let _ = msg.ack().await;
                });
                futures::future::join_all(ack_futures).await;
            });
        }
        Ok(())
    }
}
