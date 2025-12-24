//! # Pub/Sub Native Client Implementation
//!
//! This module provides the gRPC-based Pub/Sub client for the Spark connector.
//! It is architected for high performance, using a background task to manage
//! a long-lived `StreamingPull` connection for low-latency ingestion.
//!
//! Why a separate module?
//! - **Separation of Concerns**: Decouples gRPC/Tokio logic from JNI bridge code.
//! - **Async Orchestration**: Allows for background buffering of messages independently of JNI calls.
//! - **Testability**: Enables easier mocking or direct unit testing of the data plane.

use google_cloud_googleapis::pubsub::v1::subscriber_client::SubscriberClient;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_token::TokenSourceProvider;
use tonic::{transport::Channel, Request};
use tonic::metadata::MetadataValue;
use tokio::sync::mpsc::{self, Receiver};
use tokio::time::Duration;
use std::str::FromStr;

use google_cloud_googleapis::pubsub::v1::ReceivedMessage;

use google_cloud_googleapis::pubsub::v1::StreamingPullRequest;
use tokio::sync::mpsc::Sender;

/// A low-latency subscriber client that manages a background `StreamingPull` task.
/// It uses a bounded MPSC channel to buffer messages received from the gRPC stream.
pub struct PubSubClient {
    /// Receiver for buffered messages from the background task.
    receiver: Receiver<ReceivedMessage>,
    /// Sender for requests (Acks) to the background task.
    sender: Sender<StreamingPullRequest>,
    /// The full subscription resource name.
    pub subscription_name: String,
}

// ... (skip create_channel_and_header)

impl PubSubClient {
    /// Creates a new `PubSubClient`, establishes a gRPC channel, and spawns the background stream task.
    pub async fn new(project_id: &str, subscription_id: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        eprintln!("Rust: PubSubClient::new called for project: {}, subscription: {}", project_id, subscription_id);
        let (channel, header_val) = create_channel_and_header().await?;

        let mut client = SubscriberClient::with_interceptor(channel, move |mut req: Request<()>| {
            if let Some(val) = &header_val {
                req.metadata_mut().insert("authorization", val.clone());
            }
            Ok(req)
        });

        // 3. Start StreamingPull
        let (tx, rx) = mpsc::channel(1000);
        
        let full_sub_name = if subscription_id.contains('/') {
            subscription_id.to_string()
        } else {
            format!("projects/{}/subscriptions/{}", project_id, subscription_id)
        };

        let (ext_tx, mut ext_rx) = tokio::sync::mpsc::channel::<StreamingPullRequest>(100);
        let sub_name_for_task = full_sub_name.clone();
        
        tokio::spawn(async move {
            eprintln!("Rust: Background StreamingPull task started");
            let mut backoff_secs = 1;
            
            loop {
                let sub_name = sub_name_for_task.clone();
                let (grpc_tx, grpc_rx) = tokio::sync::mpsc::channel(100);
                let request_stream = tokio_stream::wrappers::ReceiverStream::new(grpc_rx);
                
                // Send initial request
                let init_req = StreamingPullRequest {
                    subscription: sub_name,
                    stream_ack_deadline_seconds: 60,
                    client_id: "rust-spark-connector".to_string(),
                    max_outstanding_messages: 1000,
                    max_outstanding_bytes: 10 * 1024 * 1024,
                    ..Default::default()
                };

                if let Err(e) = grpc_tx.send(init_req).await {
                     eprintln!("Rust: Failed to send init request to gRPC stream: {:?}", e);
                     tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                     backoff_secs = std::cmp::min(backoff_secs * 2, 60);
                     continue;
                }

                let response_stream = client.streaming_pull(Request::new(request_stream)).await;
                
                match response_stream {
                    Ok(response) => {
                        eprintln!("Rust: StreamingPull established. Resetting backoff.");
                        backoff_secs = 1;
                        let mut stream = response.into_inner();
                        
                        // Concurrent loop to read from external RX and write to gRPC TX,
                        // while reading from gRPC stream and writing to internal TX.
                        loop {
                            tokio::select! {
                                resp_res = stream.message() => {
                                    match resp_res {
                                        Ok(Some(resp)) => {
                                            for recv_msg in resp.received_messages {
                                                if tx.send(recv_msg).await.is_err() { return; }
                                            }
                                        }
                                        _ => break, // Stream closed or error
                                    }
                                }
                                ext_opt = ext_rx.recv() => {
                                    match ext_opt {
                                        Some(ext_req) => {
                                            if grpc_tx.send(ext_req).await.is_err() {
                                                break;
                                            }
                                        }
                                        None => return, // External channel closed
                                    }
                                }
                            }
                        }
                        eprintln!("Rust: StreamingPull stream ended. Retrying...");
                    },
                    Err(e) => {
                        eprintln!("Rust: StreamingPull failed: {:?}. Retrying in {}s", e, backoff_secs);
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = std::cmp::min(backoff_secs * 2, 60);
                    }
                }
            }
        });

        Ok(Self {
            receiver: rx,
            sender: ext_tx, 
            subscription_name: full_sub_name,
        })
    }

    /// Drains up to `batch_size` messages from the internal buffer.
    /// This is non-blocking and will return early if the buffer is empty or the timeout is reached.
    pub async fn fetch_batch(&mut self, batch_size: usize) -> Vec<ReceivedMessage> {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
             match tokio::time::timeout(Duration::from_millis(3000), self.receiver.recv()).await {
                 Ok(Some(msg)) => {
                     eprintln!("Rust: Received message from channel: {}", msg.ack_id);
                     batch.push(msg);
                 },
                 Ok(None) => {
                     eprintln!("Rust: Receiver channel closed");
                     break;
                 },
                 Err(_) => break, // timeout
             }
        }
        batch
    }
    
    /// Queues a list of Ack IDs for acknowledgment via the background `StreamingPull` stream.
    pub async fn acknowledge(&self, ack_ids: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        eprintln!("Rust: Sending ack request for {} ids", ack_ids.len());
        if ack_ids.is_empty() {
            return Ok(());
        }
        let req = StreamingPullRequest {
            ack_ids,
            ..Default::default()
        };
        // We use the sender to send this request into the stream
        self.sender.send(req).await.map_err(|e| format!("Failed to send Ack request: {}", e))?;
        eprintln!("Rust: Ack request sent to stream");
        Ok(())
    }
}

use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;

/// Global connection pool to share gRPC channels across multiple JNI instances.
static CONNECTION_POOL: Lazy<Mutex<HashMap<String, Channel>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Type alias for the off-heap reservoir structure.
type AckReservoirMap = HashMap<String, HashMap<String, Vec<String>>>;

/// Global off-heap reservoir for ack_ids waiting for Spark commit signals.
/// Map<BatchId, Map<SubscriptionName, Vec<AckId>>>
pub(crate) static ACK_RESERVOIR: Lazy<Mutex<AckReservoirMap>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Starts the background deadline manager if not already running.
pub fn start_deadline_manager(rt: &tokio::runtime::Runtime) {
    static STARTED: Lazy<std::sync::atomic::AtomicBool> = Lazy::new(|| std::sync::atomic::AtomicBool::new(false));
    if STARTED.compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_ok() {
        eprintln!("Rust: Starting background deadline manager");
        rt.spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                
                let snapshot: Vec<(String, Vec<String>)> = {
                    let reservoir = ACK_RESERVOIR.lock().unwrap();
                    let mut all = Vec::new();
                    // We iterate over batches, and then subscriptions
                    for (_batch_id, subs) in reservoir.iter() {
                        for (sub, ids) in subs {
                            if !ids.is_empty() {
                                all.push((sub.clone(), ids.clone()));
                            }
                        }
                    }
                    all
                };

                for (sub, ids) in snapshot {
                    let (channel, header_val) = match create_channel_and_header().await {
                        Ok(res) => res,
                        Err(e) => {
                            eprintln!("Rust DeadlineMgr: Failed to get channel: {:?}", e);
                            continue;
                        }
                    };
                    let mut client = google_cloud_googleapis::pubsub::v1::subscriber_client::SubscriberClient::new(channel);
                    
                    let req = google_cloud_googleapis::pubsub::v1::ModifyAckDeadlineRequest {
                        subscription: sub,
                        ack_ids: ids,
                        ack_deadline_seconds: 30, // Extend by 30s
                    };
                    let mut request = Request::new(req);
                    if let Some(val) = header_val {
                        request.metadata_mut().insert("authorization", val);
                    }
                    if let Err(e) = client.modify_ack_deadline(request).await {
                        eprintln!("Rust DeadlineMgr: Failed to extend deadlines: {:?}", e);
                    }
                }
            }
        });
    }
}

/// Helper to create a gRPC channel and optional Auth header.
/// If `PUBSUB_EMULATOR_HOST` is set, it bypasses Auth and connects to the emulator.
async fn create_channel_and_header() -> Result<(Channel, Option<MetadataValue<tonic::metadata::Ascii>>), Box<dyn std::error::Error + Send + Sync>> {
    let emulator_host = std::env::var("PUBSUB_EMULATOR_HOST").ok();
    let endpoint = emulator_host.as_ref().map(|h| format!("http://{}", h)).unwrap_or_else(|| "https://pubsub.googleapis.com".to_string());
    
    // Check pool first
    let channel = {
        let pool = CONNECTION_POOL.lock().unwrap();
        pool.get(&endpoint).cloned()
    };

    let channel = if let Some(ch) = channel {
        ch
    } else {
        eprintln!("Rust: Creating new gRPC channel for {}", endpoint);
        let ch = Channel::from_shared(endpoint.clone())?
            .connect()
            .await?;
        let mut pool = CONNECTION_POOL.lock().unwrap();
        pool.insert(endpoint, ch.clone());
        ch
    };

    let header_val = if emulator_host.is_some() {
        None
    } else {
        let config = google_cloud_auth::project::Config::default();
        let ts = google_cloud_auth::token::DefaultTokenSourceProvider::new(config).await?;
        let token_source = ts.token_source();
        let token = token_source.token().await?;
        let val = MetadataValue::from_str(&format!("Bearer {}", token))?;
        Some(val)
    };

    Ok((channel, header_val))
}

/// High-performance publisher client that sends batches of messages via gRPC.
/// It uses a background task to decouple Spark execution from network latency.
pub struct PublisherClient {
    tx: Sender<Vec<PubsubMessage>>,
}

impl PublisherClient {
    pub async fn new(project_id: &str, topic_id: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (channel, header_val) = create_channel_and_header().await?;
        let mut client = google_cloud_googleapis::pubsub::v1::publisher_client::PublisherClient::new(channel);
        
        let full_topic_name = if topic_id.contains('/') {
            topic_id.to_string()
        } else {
            format!("projects/{}/topics/{}", project_id, topic_id)
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<PubsubMessage>>(100);
        let topic = full_topic_name.clone();
        let header_val_clone = header_val.clone(); // Clone header_val for the spawned task

        tokio::spawn(async move {
            while let Some(messages) = rx.recv().await {
                if messages.is_empty() { continue; }
                
                let req = google_cloud_googleapis::pubsub::v1::PublishRequest {
                    topic: topic.clone(),
                    messages,
                };
                
                let mut request = Request::new(req);
                if let Some(val) = &header_val_clone { // Use the cloned header_val
                    request.metadata_mut().insert("authorization", val.clone());
                }
                
                if let Err(e) = client.publish(request).await {
                    eprintln!("Rust: Async Publish failed: {:?}", e);
                }
            }
            eprintln!("Rust: Publisher background task ended");
        });

        Ok(Self { tx })
    }
    
    /// Publishes a batch of messages to the configured topic in a single gRPC request.
    pub async fn publish_batch(&mut self, messages: Vec<PubsubMessage>) -> Result<(), Box<dyn std::error::Error>> {
        if messages.is_empty() {
             return Ok(());
        }
        self.tx.send(messages).await.map_err(|e| format!("Failed to queue batch for publish: {}", e))?;
        Ok(())
    }
    
    pub async fn flush(&self) {
        // Since we use MPSC channel, messages are queued. 
        // For a full flush, we'd need to wait for the channel to be empty or use a shutdown signal.
        // For now, Spark's task completion will wait for JNI calls to finish.
    }
}
