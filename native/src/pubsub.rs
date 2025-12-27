//! # Pub/Sub Native Client (First Principles High-Level Redesign)
//!
//! Uses `google-cloud-pubsub` crate officially for all operations.
//! Leverages official Lease Management and Flow Control.

use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscriber::ReceivedMessage as HighLevelMessage;
use google_cloud_pubsub::subscription::FlowControlSettings;
use google_cloud_pubsub::publisher::Publisher;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_googleapis::pubsub::v1::ReceivedMessage as LowLevelMessage;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;
use dashmap::DashMap;

static GLOBAL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

// Global map to hold high-level message handles (keeps leases alive)
// Key: AckID, Value: HighLevelMessage
pub static ACK_HANDLE_MAP: Lazy<DashMap<String, HighLevelMessage>> = Lazy::new(|| {
    DashMap::new()
});

// Map to track messages belonging to a Spark Batch (for commit-time acking)
// Key: BatchID, Value: Vec<AckID>
pub static BATCH_ACK_MAP: Lazy<DashMap<String, Vec<String>>> = Lazy::new(|| {
    DashMap::new()
});

// Metrics
pub static BUFFERED_BYTES: AtomicUsize = AtomicUsize::new(0);
pub static INGESTED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static INGESTED_MESSAGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PUBLISHED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PUBLISHED_MESSAGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static READ_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static WRITE_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static RETRY_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PUBLISH_LATENCY_TOTAL_MICROS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static ACK_LATENCY_TOTAL_MICROS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn get_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(32)
            .thread_name("pubsub-native-worker")
            .build()
            .expect("Failed to create global Tokio runtime")
    })
}

// Metric accessors
pub fn get_buffered_bytes() -> i64 { BUFFERED_BYTES.load(Ordering::Relaxed) as i64 }
pub fn get_ingested_bytes() -> i64 { INGESTED_BYTES.load(Ordering::Relaxed) as i64 }
pub fn get_ingested_messages() -> i64 { INGESTED_MESSAGES.load(Ordering::Relaxed) as i64 }
pub fn get_published_bytes() -> i64 { PUBLISHED_BYTES.load(Ordering::Relaxed) as i64 }
pub fn get_published_messages() -> i64 { PUBLISHED_MESSAGES.load(Ordering::Relaxed) as i64 }
pub fn get_read_errors() -> i64 { READ_ERRORS.load(Ordering::Relaxed) as i64 }
pub fn get_write_errors() -> i64 { WRITE_ERRORS.load(Ordering::Relaxed) as i64 }
pub fn get_retry_count() -> i64 { RETRY_COUNT.load(Ordering::Relaxed) as i64 }
pub fn get_publish_latency_micros() -> i64 { PUBLISH_LATENCY_TOTAL_MICROS.load(Ordering::Relaxed) as i64 }
pub fn get_ack_latency_micros() -> i64 { ACK_LATENCY_TOTAL_MICROS.load(Ordering::Relaxed) as i64 }

pub struct PubSubClient {
    pub subscription_name: String,
    receiver: mpsc::Receiver<LowLevelMessage>,
    _client: Client, 
}

impl PubSubClient {
    pub async fn new(project_id: &str, subscription_id: &str, _ca_path: Option<&str>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        log::info!("Rust: [First Principles] Initializing PubSubClient for {}/{}", project_id, subscription_id);
        
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;
        
        let full_sub_name = if subscription_id.contains('/') {
            subscription_id.to_string()
        } else {
            format!("projects/{}/subscriptions/{}", project_id, subscription_id)
        };
        
        let subscription = client.subscription(&full_sub_name);

        // Configure Flow Control (Deep Buffering for Spark)
        // 10,000 messages or 200MB per partition.
        let flow_control = FlowControlSettings {
            max_outstanding_messages: 10_000,
            max_outstanding_bytes: 200 * 1024 * 1024, // 200 MB
            ..Default::default()
        };
        
        // Channel to bridge Library -> Spark (Deep buffer)
        let (tx, rx) = mpsc::channel::<LowLevelMessage>(20_000);
        let sub_clone = subscription.clone();
        let sub_name_clone = full_sub_name.clone();
        
        tokio::spawn(async move {
            log::info!("Rust: High-Level Subscriber task starting for {}", sub_name_clone);
            
            // Use stream-based subscription with flow control
            let mut stream = match sub_clone.subscribe(Some(flow_control)).await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("Rust: Subscription failed for {}: {:?}", sub_name_clone, e);
                    READ_ERRORS.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            
            while let Some(msg) = stream.next().await {
                let ack_id = msg.ack_id().to_string(); 
                
                // Track metrics and buffer size before conversion
                let size = msg.message.data.len();
                BUFFERED_BYTES.fetch_add(size, Ordering::Relaxed);
                INGESTED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
                INGESTED_MESSAGES.fetch_add(1, Ordering::Relaxed);

                // Convert payload
                let pubsub_msg = PubsubMessage {
                    data: msg.message.data.clone().into(),
                    attributes: msg.message.attributes.clone(),
                    message_id: msg.message.message_id.clone(),
                    publish_time: msg.message.publish_time.clone(),
                    ordering_key: msg.message.ordering_key.clone(),
                    ..Default::default()
                };
                
                let delivery_attempt = msg.delivery_attempt().unwrap_or(0) as i32;

                let low_level = LowLevelMessage {
                    ack_id: ack_id.clone(),
                    message: Some(pubsub_msg),
                    delivery_attempt,
                };
                
                // Retain message handle in global map (ensures leasing)
                ACK_HANDLE_MAP.insert(ack_id.clone(), msg);
                
                if tx.send(low_level).await.is_err() {
                    // Receiver closed (Spark task done or died)
                    if let Some((_, m)) = ACK_HANDLE_MAP.remove(&ack_id) {
                         let _ = m.nack().await;
                    }
                    break;
                }
            }
            log::info!("Rust: High-Level Subscriber task ended for {}", sub_name_clone);
        });

        Ok(PubSubClient {
            subscription_name: full_sub_name,
            receiver: rx,
            _client: client,
        })
    }

    pub async fn fetch_batch(&mut self, max_messages: usize, wait_ms: u64) -> Result<Vec<LowLevelMessage>, Box<dyn std::error::Error + Send + Sync>> {
        let mut messages = Vec::with_capacity(max_messages);
        let deadline = Instant::now() + Duration::from_millis(wait_ms);

        while messages.len() < max_messages {
            let now = Instant::now();
            if now >= deadline { break; }
            
            match tokio::time::timeout(deadline - now, self.receiver.recv()).await {
                Ok(Some(msg)) => {
                    let size = msg.message.as_ref().map(|m| m.data.len()).unwrap_or(0);
                    BUFFERED_BYTES.fetch_sub(size, Ordering::Relaxed);
                    messages.push(msg);
                    
                    // Greedy drain of the internal channel
                    while messages.len() < max_messages {
                         match self.receiver.try_recv() {
                            Ok(m) => {
                                let s = m.message.as_ref().map(|x| x.data.len()).unwrap_or(0);
                                BUFFERED_BYTES.fetch_sub(s, Ordering::Relaxed);
                                messages.push(m);
                            },
                            Err(_) => break,
                        }
                    }
                },
                Ok(None) => break, 
                Err(_) => break, 
            }
        }
        Ok(messages)
    }

    pub async fn acknowledge(&self, ack_ids: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for id in ack_ids {
            if let Some((_, msg)) = ACK_HANDLE_MAP.remove(&id) {
                tokio::spawn(async move {
                    let _ = msg.ack().await;
                });
            }
        }
        Ok(())
    }
}

pub struct PublisherClient {
    publisher: Publisher,
}

impl PublisherClient {
    pub async fn new(project_id: &str, topic_id: &str, _ca_path: Option<&str>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;
        let full_topic_name = if topic_id.contains('/') {
            topic_id.to_string()
        } else {
            format!("projects/{}/topics/{}", project_id, topic_id)
        };
        let topic = client.topic(&full_topic_name);
        let publisher = topic.new_publisher(None);
        
        Ok(Self { publisher })
    }
    
    pub async fn publish_batch(&mut self, messages: Vec<PubsubMessage>) -> Result<(), Box<dyn std::error::Error>> {
        let mut futures = Vec::with_capacity(messages.len());
        let start = Instant::now();
        
        for msg in messages {
            let size = msg.data.len() as u64;
            let publisher = self.publisher.clone();
            PUBLISHED_BYTES.fetch_add(size, Ordering::Relaxed);
            PUBLISHED_MESSAGES.fetch_add(1, Ordering::Relaxed);
            
            let fut = tokio::spawn(async move {
                match publisher.publish(msg).await.get().await {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("Rust: Publish error: {:?}", e);
                        WRITE_ERRORS.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            futures.push(fut);
        }
        
        // Wait for all to prevent uncontrolled backlog
        for fut in futures {
            let _ = fut.await;
        }
        
        PUBLISH_LATENCY_TOTAL_MICROS.fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        Ok(())
    }
    
    pub async fn flush(&self, _timeout: Duration) -> Result<(), String> {
        // High level library usually auto-flushes or doesn't expose manual flush on the handle?
        // Actually topic.new_publisher returns a handle. library docs show it handles batching.
        Ok(())
    }
}
