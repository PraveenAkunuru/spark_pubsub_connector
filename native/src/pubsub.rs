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
    /// Creates a new `PubSubClient`, establishes a gRPC channel, and spawns the background stream task.
    pub async fn new(project_id: &str, subscription_id: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        log::info!("Rust: PubSubClient::new called for project: {}, subscription: {}", project_id, subscription_id);
        let (channel, header_val) = create_channel_and_header().await?;

        let full_sub_name = if subscription_id.contains('/') {
            subscription_id.to_string()
        } else {
            format!("projects/{}/subscriptions/{}", project_id, subscription_id)
        };

        let mut client = SubscriberClient::with_interceptor(channel, move |mut req: Request<()>| {
            if let Some(val) = &header_val {
                req.metadata_mut().insert("authorization", val.clone());
            }
            Ok(req)
        });

        // 1. Synchronous Validation: Check if subscription exists
        let check_req = google_cloud_googleapis::pubsub::v1::GetSubscriptionRequest {
            subscription: full_sub_name.clone(),
        };
        match client.get_subscription(Request::new(check_req)).await {
            Ok(_) => log::info!("Rust: Subscription validated: {}", full_sub_name),
            Err(e) => {
                log::error!("Rust: Subscription validation failed: {:?}", e);
                return Err(Box::new(e));
            }
        }

        // 3. Start StreamingPull
        let (tx, rx) = mpsc::channel(1000);
        
        // We moved full_sub_name definition up

        let (ext_tx, mut ext_rx) = tokio::sync::mpsc::channel::<StreamingPullRequest>(100);
        let sub_name_for_task = full_sub_name.clone();
        
        tokio::spawn(async move {
            log::info!("Rust: Background StreamingPull task started");
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
                     log::error!("Rust: Failed to send init request to gRPC stream: {:?}", e);
                     tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                     backoff_secs = std::cmp::min(backoff_secs * 2, 60);
                     continue;
                }

                let response_stream = client.streaming_pull(Request::new(request_stream)).await;
                
                match response_stream {
                    Ok(response) => {
                        log::info!("Rust: StreamingPull established. Resetting backoff.");
                        backoff_secs = 1;
                        let mut stream = response.into_inner();
                        
                        // Buffer for messages that we pulled but haven't pushed to Spark yet due to backpressure
                        let mut pending_messages: std::collections::VecDeque<ReceivedMessage> = std::collections::VecDeque::new();

                        loop {
                            let tx_full = pending_messages.len() >= 2000;

                            tokio::select! {
                                ext_opt = ext_rx.recv() => {
                                    match ext_opt {
                                        Some(ext_req) => {
                                            if grpc_tx.send(ext_req).await.is_err() {
                                                break;
                                            }
                                        }
                                        None => return, 
                                    }
                                }
                                resp_res = stream.message(), if !tx_full => {
                                    match resp_res {
                                        Ok(Some(resp)) => {
                                            for msg in resp.received_messages {
                                                pending_messages.push_back(msg);
                                            }
                                        }
                                        Ok(None) => break, 
                                        Err(e) => {
                                            log::error!("Rust: gRPC Stream error: {:?}", e);
                                            break;
                                        }
                                    }
                                }
                                res = tx.reserve(), if !pending_messages.is_empty() => {
                                    match res {
                                        Ok(permit) => {
                                            if let Some(msg) = pending_messages.pop_front() {
                                                permit.send(msg);
                                            }
                                        }
                                        Err(_) => return,
                                    }
                                }
                            }
                        }
                        log::warn!("Rust: StreamingPull stream ended. Retrying...");
                    },
                    Err(e) => {
                        log::warn!("Rust: StreamingPull failed: {:?}. Retrying in {}s", e, backoff_secs);
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
    /// 
    /// This method waits for up to 3 seconds for messages to arrive. If the buffer is empty
    /// and the background task is still alive, it returns an empty vector.
    /// Returns an error if the receiver channel is closed (background task died).
    pub async fn fetch_batch(&mut self, batch_size: usize) -> Result<Vec<ReceivedMessage>, String> {
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
             match tokio::time::timeout(Duration::from_millis(3000), self.receiver.recv()).await {
                 Ok(Some(msg)) => {
                     // eprintln!("Rust: Received message from channel: {}", msg.ack_id);
                     batch.push(msg);
                 },
                 Ok(None) => {
                     log::error!("Rust: Receiver channel closed (background task died).");
                     if batch.is_empty() {
                         return Err("Background task died".to_string());
                     }
                     break; // Return partial batch if we have any
                 },
                 Err(_) => break, // timeout
             }
        }
        Ok(batch)
    }
    
    /// Queues a list of Ack IDs for acknowledgment via the background `StreamingPull` stream.
    /// 
    /// This is an asynchronous operation that merely sends the IDs to the background task.
    /// It does not wait for the server to confirm receipt.
    pub async fn acknowledge(&self, ack_ids: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        log::debug!("Rust: Sending ack request for {} ids", ack_ids.len());
        if ack_ids.is_empty() {
            return Ok(());
        }
        let req = StreamingPullRequest {
            ack_ids,
            ..Default::default()
        };
        // We use the sender to send this request into the stream
        self.sender.send(req).await.map_err(|e| format!("Failed to send Ack request: {}", e))?;
        log::debug!("Rust: Ack request sent to stream");
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

/// Starts the background deadline manager.
/// This function should be called once per Runtime to ensure liveness.
/// Starts the background deadline manager.
/// This function should be called once per Runtime to ensure liveness.
pub fn start_deadline_manager(rt: &tokio::runtime::Runtime) {
    log::info!("Rust: Starting background deadline manager for runtime");
    rt.spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            let snapshot: Vec<(String, Vec<String>)> = {
                let reservoir = ACK_RESERVOIR.lock().unwrap_or_else(|e| e.into_inner());
                let mut all = Vec::new();
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
                        log::warn!("Rust DeadlineMgr: Failed to get channel: {:?}", e);
                        continue;
                    }
                };
                
                let mut client = SubscriberClient::new(channel);
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
                    log::warn!("Rust DeadlineMgr: Failed to extend deadlines: {:?}", e);
                }
            }
        }
    });
}

/// Helper to create a gRPC channel and optional Auth header.
/// If `PUBSUB_EMULATOR_HOST` is set, it bypasses Auth and connects to the emulator.
async fn create_channel_and_header() -> Result<(Channel, Option<MetadataValue<tonic::metadata::Ascii>>), Box<dyn std::error::Error + Send + Sync>> {
    let emulator_host = std::env::var("PUBSUB_EMULATOR_HOST").ok();
    let endpoint = emulator_host.as_ref().map(|h| format!("http://{}", h)).unwrap_or_else(|| "https://pubsub.googleapis.com".to_string());
    
    // Check pool first
    let channel = {
        let pool = CONNECTION_POOL.lock().unwrap_or_else(|e| e.into_inner());
        pool.get(&endpoint).cloned()
    };

    let channel = if let Some(ch) = channel {
        ch
    } else {
        log::debug!("Rust: Creating new gRPC channel for {}", endpoint);
        let ch = Channel::from_shared(endpoint.clone())?
            .connect()
            .await?;
        let mut pool = CONNECTION_POOL.lock().unwrap_or_else(|e| e.into_inner());
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

/// Command sent to the background publisher task.
enum WriterCommand {
    Publish(Vec<PubsubMessage>),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// High-performance publisher client that sends batches of messages via gRPC.
/// It uses a background task to decouple Spark execution from network latency.
pub struct PublisherClient {
    tx: Sender<WriterCommand>,
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

        let (tx, mut rx) = tokio::sync::mpsc::channel::<WriterCommand>(100);
        let topic = full_topic_name.clone();
        let header_val_clone = header_val.clone(); // Clone header_val for the spawned task

        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    WriterCommand::Publish(messages) => {
                         if messages.is_empty() { continue; }
                        
                        let req = google_cloud_googleapis::pubsub::v1::PublishRequest {
                            topic: topic.clone(),
                            messages,
                        };
                        
                        // Exponential Backoff for resilience against transient errors.
                        // Starts at 100ms, caps at 60s.
                        // Essential for handling "ServiceUnavailable" or "TransportError" during high load or outages.
                        let mut backoff_millis = 100;
                        let max_backoff = 60000; // 60s

                        loop {
                            let mut request = Request::new(req.clone());
                            if let Some(val) = &header_val_clone {
                                request.metadata_mut().insert("authorization", val.clone());
                            }
                            
                            match client.publish(request).await {
                                Ok(_) => break, // Success
                                Err(e) => {
                                    let code = e.code();
                                    if code == tonic::Code::NotFound || code == tonic::Code::PermissionDenied || code == tonic::Code::InvalidArgument {
                                        log::error!("Rust: Fatal Publish Error: {:?}. Stopping background task.", e);
                                        return; // Exit the background task immediately (closes rx)
                                    }

                                    log::warn!("Rust: Async Publish failed: {:?}. Retrying in {}ms", e, backoff_millis);
                                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_millis)).await;
                                    backoff_millis = std::cmp::min(backoff_millis * 2, max_backoff);
                                }
                            }
                        }
                    },
                    WriterCommand::Flush(ack_tx) => {
                        // Because we process commands sequentially in this loop,
                        // by the time we reach here, all previous Publish commands are finished.
                        let _ = ack_tx.send(());
                        log::info!("Rust: Flush completed.");
                    }
                }
            }
            log::info!("Rust: Publisher background task ended");
        });

        Ok(Self { tx })
    }
    
    /// Publishes a batch of messages to the configured topic in a single gRPC request.
    /// 
    /// This method queues the batch for the background thread to handle.
    pub async fn publish_batch(&mut self, messages: Vec<PubsubMessage>) -> Result<(), Box<dyn std::error::Error>> {
        if messages.is_empty() {
             return Ok(());
        }
        self.tx.send(WriterCommand::Publish(messages)).await.map_err(|e| format!("Failed to queue batch for publish: {}", e))?;
        Ok(())
    }
    
    /// Flushes all pending messages by sending a Flush command and waiting for it to be processed.
    /// 
    /// This method blocks (asynchronously) until all previously queued messages have been attempted.
    pub async fn flush(&self, timeout: Duration) -> Result<(), String> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self.tx.send(WriterCommand::Flush(ack_tx)).await {
            log::warn!("Rust: Failed to send flush command: {:?}", e);
            return Err("Failed to send flush command (background task died?)".to_string());
        }
        
        match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => {
                log::error!("Rust: Flush wait failed (channel closed): {:?}", e);
                Err("Flush channel closed (background task died?)".to_string())
            },
            Err(_) => {
                log::error!("Rust: Flush timed out after {:?}.", timeout);
                Err("Flush timed out".to_string())
            }
        }
    }
}
