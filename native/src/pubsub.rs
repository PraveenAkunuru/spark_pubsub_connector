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
use tonic::{transport::Channel, transport::ClientTlsConfig, transport::Certificate, Request};
use tonic::metadata::MetadataValue;
use tokio::sync::mpsc::{self, Receiver};
use tokio::time::{Duration, Instant};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

use google_cloud_googleapis::pubsub::v1::ReceivedMessage;

use google_cloud_googleapis::pubsub::v1::StreamingPullRequest;
use tokio::sync::mpsc::Sender;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

static GLOBAL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

static CONNECTION_POOL: once_cell::sync::Lazy<std::sync::Mutex<std::collections::HashMap<String, Channel>>> = once_cell::sync::Lazy::new(|| {
    std::sync::Mutex::new(std::collections::HashMap::new())
});

/// Global registry for persistent PubSubClient instances.
/// Key is (subscription_name, partition_id), value is the persistent client.
pub static CLIENT_REGISTRY: once_cell::sync::Lazy<dashmap::DashMap<(String, i32), PubSubClient>> = once_cell::sync::Lazy::new(|| {
    dashmap::DashMap::new()
});

/// Global cache for the token source to avoid redundant metadata server hits during parallel initialization.
static GLOBAL_TOKEN_SOURCE: OnceLock<google_cloud_auth::token::DefaultTokenSourceProvider> = OnceLock::new();

type AckReservoir = std::sync::Mutex<
    std::collections::HashMap<
        String,
        (Instant, std::collections::HashMap<String, Vec<String>>),
    >,
>;

pub static ACK_RESERVOIR: once_cell::sync::Lazy<AckReservoir> = once_cell::sync::Lazy::new(|| {
    std::sync::Mutex::new(std::collections::HashMap::new())
});

pub fn ack_reservoir_instant_now() -> Instant {
    Instant::now()
}

/// Global counter for bytes currently buffered in the native layer (waiting for Spark to fetch).
/// This provides visibility into off-heap memory usage.
static BUFFERED_BYTES: AtomicUsize = AtomicUsize::new(0);

// Cumulative throughput metrics (Ingest / Reading)
static INGESTED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static INGESTED_MESSAGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// Cumulative throughput metrics (Publish / Writing)
static PUBLISHED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static PUBLISHED_MESSAGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// Health & Error Metrics
static READ_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static WRITE_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static RETRY_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// Latency Metrics (Cumulative micros)
static PUBLISH_LATENCY_TOTAL_MICROS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ACK_LATENCY_TOTAL_MICROS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Returns a reference to the global Tokio runtime.
/// It is lazily initialized on the first call.
pub fn get_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(16)
            .thread_name("pubsub-native-worker")
            .build()
            .expect("Failed to create global Tokio runtime")
    })
}

/// Returns the current estimated size of buffered messages in bytes.
pub fn get_buffered_bytes() -> i64 {
    BUFFERED_BYTES.load(Ordering::Relaxed) as i64
}

pub fn get_ingested_bytes() -> i64 {
    INGESTED_BYTES.load(Ordering::Relaxed) as i64
}

pub fn get_ingested_messages() -> i64 {
    INGESTED_MESSAGES.load(Ordering::Relaxed) as i64
}

pub fn get_published_bytes() -> i64 {
    PUBLISHED_BYTES.load(Ordering::Relaxed) as i64
}

pub fn get_published_messages() -> i64 {
    PUBLISHED_MESSAGES.load(Ordering::Relaxed) as i64
}

pub fn get_read_errors() -> i64 {
    READ_ERRORS.load(Ordering::Relaxed) as i64
}

pub fn get_write_errors() -> i64 {
    WRITE_ERRORS.load(Ordering::Relaxed) as i64
}

pub fn get_retry_count() -> i64 {
    RETRY_COUNT.load(Ordering::Relaxed) as i64
}

pub fn get_publish_latency_micros() -> i64 {
    PUBLISH_LATENCY_TOTAL_MICROS.load(Ordering::Relaxed) as i64
}

pub fn get_ack_latency_micros() -> i64 {
    ACK_LATENCY_TOTAL_MICROS.load(Ordering::Relaxed) as i64
}

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

impl PubSubClient {
    /// Creates a new `PubSubClient`, establishes a gRPC channel, and spawns the background stream task.
    pub async fn new(project_id: &str, subscription_id: &str, ca_path: Option<&str>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        log::info!("Rust: PubSubClient::new called for project: {}, subscription: {}", project_id, subscription_id);
        let (channel, header_val) = create_channel_and_header(ca_path).await?;

        let full_sub_name = if subscription_id.contains("/subscriptions/") {
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
            Ok(_) => {
                log::info!("Rust: Subscription validated: {}", full_sub_name);
            },
            // ... matches existing logic ...
            Err(e) => {
                log::error!("Rust: Subscription validation failed: {:?}", e);
                return Err(Box::new(e));
            }
        }

        // 3. Start StreamingPull
        let (tx, rx) = mpsc::channel(20000); // Increased for high pre-fetch
        
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
                    max_outstanding_messages: 50000,
                    max_outstanding_bytes: 500 * 1024 * 1024, // 500MB headroom
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
                            let tx_full = pending_messages.len() >= 100000;

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
                                            if !resp.received_messages.is_empty() {
                                                log::info!("Rust: Received {} messages from StreamingPull", resp.received_messages.len());
                                            }
                                            for msg in resp.received_messages {
                                                // INSTRUMENTATION
                                                let size = msg.message.as_ref().map(|m| m.data.len()).unwrap_or(0);
                                                BUFFERED_BYTES.fetch_add(size, Ordering::Relaxed);
                                                INGESTED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
                                                INGESTED_MESSAGES.fetch_add(1, Ordering::Relaxed);
                                                
                                                pending_messages.push_back(msg);
                                            }
                                        }
                                        Ok(None) => break, 
                                        Err(e) => {
                                            log::error!("Rust: gRPC Stream error: {:?}", e);
                                            READ_ERRORS.fetch_add(1, Ordering::Relaxed);
                                            break;
                                        }
                                    }
                                }
                                res = tx.reserve(), if !pending_messages.is_empty() => {
                                    match res {
                                        Ok(permit) => {
                                            if let Some(msg) = pending_messages.pop_front() {
                                                let size = msg.message.as_ref().map(|m| m.data.len()).unwrap_or(0);
                                                crate::pubsub::BUFFERED_BYTES.fetch_sub(size, std::sync::atomic::Ordering::Relaxed);
                                                permit.send(msg);
                                                
                                                // Aggressively drain remaining while space allows
                                                while let Some(next_msg) = pending_messages.front() {
                                                    let n_size = next_msg.message.as_ref().map(|m| m.data.len()).unwrap_or(0);
                                                    match tx.try_send(pending_messages.pop_front().unwrap()) {
                                                        Ok(_) => {
                                                            crate::pubsub::BUFFERED_BYTES.fetch_sub(n_size, std::sync::atomic::Ordering::Relaxed);
                                                        }
                                                        Err(_) => break, // tx is full
                                                    }
                                                }
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
                        let code = e.code();
                        let mut backoff_millis = backoff_secs * 1000;
                        
                        if code == tonic::Code::ResourceExhausted {
                            log::warn!("Rust: StreamingPull quota exceeded (RESOURCE_EXHAUSTED). Using aggressive backoff.");
                            backoff_millis *= 2;
                        }

                        // Add jitter (±20%)
                        let jitter = rand::Rng::gen_range(&mut rand::thread_rng(), 0.8..1.2);
                        let sleep_ms = (backoff_millis as f64 * jitter) as u64;

                        log::warn!("Rust: StreamingPull failed: {:?}. Retrying in {}ms", e, sleep_ms);
                        READ_ERRORS.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
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
    
    /// Fetches a batch of messages from the internal buffer.
    /// Waits up to `wait_ms` for the first message, then drains available messages 
    /// up to `max_messages`.
    pub async fn fetch_batch(&mut self, max_messages: usize, wait_ms: u64) -> Result<Vec<ReceivedMessage>, Box<dyn std::error::Error + Send + Sync>> {
        let mut messages = Vec::with_capacity(max_messages);
        let deadline = Instant::now() + Duration::from_millis(wait_ms);
        
        while messages.len() < max_messages && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() { break; }

            match tokio::time::timeout(remaining, self.receiver.recv()).await {
                Ok(Some(msg)) => {
                    messages.push(msg);
                    
                    // After one message arrives, try to drain all currently available without waiting
                    while messages.len() < max_messages {
                        match self.receiver.try_recv() {
                            Ok(m) => messages.push(m),
                            Err(_) => break,
                        }
                    }
                }
                Ok(None) => break, // Channel closed
                Err(_) => break,   // Timeout
            }
        }
        
        INGESTED_MESSAGES.fetch_add(messages.len() as u64, Ordering::Relaxed);
        // Estimate bytes for 1KB if we don't have exact size yet
        INGESTED_BYTES.fetch_add((messages.len() * 1024) as u64, Ordering::Relaxed);
        
        Ok(messages)
    }
    // Note: fetch_batch is single-threaded per reader, but BUFFERED_BYTES is global.
    // We already decremented when moving from pending_messages to rx in the background task?
    // Wait, the background task decrements when it sends to `self.receiver`.
    // So `fetch_batch` doesn't need to decrement BUFFERED_BYTES.
    // The message is "buffered" when it is in `pending_messages` OR `self.receiver`.
    // The background task increments when received from gRPC.
    // It should decrement when *Spark consumes it*?
    // No, `BUFFERED_BYTES` tracks native memory usage.
    // If it's in `self.receiver` (channel), it's still in native memory.
    // If passed to Spark, it moves to JVM (or copied to Arrow buffer).
    // The `fetch_batch` moves it out of `self.receiver`.
    // The background task decremented it when popping from `pending_messages` and sending to `rx`?
    // Let's check the background task logic in `new`.
    // Line 188: `BUFFERED_BYTES.fetch_sub(size, Ordering::Relaxed);` when sending to `permit` (rx).
    // So `BUFFERED_BYTES` only counts what is in `pending_messages` deque?
    // Yes, that seems to be the logic implemented.
    // Messages in the channel (rx) are technically buffered too, but the channel has a limit (1000).
    // So the unbounded growth risk is `pending_messages`.
    // So this is fine.

    /// Sends acknowledgments for the given message IDs.
    pub async fn acknowledge(&self, ack_ids: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if ack_ids.is_empty() { return Ok(()); }
        let req = StreamingPullRequest {
            ack_ids,
            // other fields empty
            ..Default::default()
        };
        // We use the same sender channel to send acks back to the streaming loop?
        // Wait, `self.sender` is `ext_tx`.
        // The background loop listens on `ext_rx`.
        // It forwards `ext_req` to `grpc_tx`.
        // YES.
        self.sender.send(req).await.map_err(|e| format!("Failed to send ack: {}", e).into())
    }

    // IMPORTANT: deadline manager needs update too OR just use default channel?
    // Deadline manager uses default for now, but should ideally reuse channel or use same CA.
    // For now I won't change deadline manager to complicate things unless I pass CA logic globally?
    // Wait, deadline manager `start_deadline_manager` needs to know about CA too if it creates new channels?
    // Yes. It calls `create_channel_and_header().await`.
    // I should probably make `create_channel_and_header` optional arg.
    // AND I should probably update `start_deadline_manager` to take optional CA path?
    // BUT `start_deadline_manager` is spawned once globally.
    // If different readers have different CA paths (unlikely in Spark executor), it's problematic.
    // Assume single CA path per executor for now?
    // Or just let deadline manager fail for custom CA for now? No, that's bad.
    // Actually, `CONNECTION_POOL` is global.
    // If I use different CA paths, I can't easily cache by endpoint alone.
    // But usually CA path is global env config.
    // I will change `start_deadline_manager` to accept `ca_path: Option<String>` (owned).
    
    // ... skipping to create_channel_and_header ...

}

// Deadline Manager needs update
pub fn start_deadline_manager(rt: &tokio::runtime::Runtime, ca_path: Option<String>) {
    log::info!("Rust: Starting background deadline manager for runtime");
    rt.spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            let snapshot: Vec<(String, Vec<String>)> = {
                 // ... existing reservoir logic ...
                let mut reservoir = ACK_RESERVOIR.lock().unwrap_or_else(|e| e.into_inner());
                
                // 1. Purge entries older than 30 minutes (prevent OOM from abandoned batches)
                let now = Instant::now();
                let ttl = Duration::from_secs(30 * 60);
                reservoir.retain(|_id, (time, _)| {
                    now.duration_since(*time) < ttl
                });

                let mut all = Vec::new();
                for (_, (_, subs)) in reservoir.iter() {
                    for (sub, ids) in subs {
                        if !ids.is_empty() {
                            all.push((sub.clone(), ids.clone()));
                        }
                    }
                }
                all
            };

            for (sub, ids) in snapshot {
                let (channel, header_val) = match create_channel_and_header(ca_path.as_deref()).await {
                    Ok(res) => res,
                    Err(e) => {
                        log::warn!("Rust DeadlineMgr: Failed to get channel: {:?}", e);
                        continue;
                    }
                };
                
                let mut client = SubscriberClient::new(channel);
                for chunk in ids.chunks(500) {
                     // ... existing ack logic ...
                     let req = google_cloud_googleapis::pubsub::v1::ModifyAckDeadlineRequest {
                        subscription: sub.clone(),
                        ack_ids: chunk.to_vec(),
                        ack_deadline_seconds: 30, // Extend by 30s
                    };
                    let mut request = Request::new(req);
                    if let Some(val) = &header_val {
                        request.metadata_mut().insert("authorization", val.clone());
                    }
                    
                    let start = std::time::Instant::now();
                    if let Err(e) = client.modify_ack_deadline(request).await {
                        log::warn!("Rust DeadlineMgr: Failed to extend deadlines: {:?}", e);
                        READ_ERRORS.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let duration = start.elapsed().as_micros() as u64;
                        ACK_LATENCY_TOTAL_MICROS.fetch_add(duration, Ordering::Relaxed);
                    }
                }
            }
        }
    });
}

/// Helper to create a gRPC channel and optional Auth header.
async fn create_channel_and_header(ca_path: Option<&str>) -> Result<(Channel, Option<MetadataValue<tonic::metadata::Ascii>>), Box<dyn std::error::Error + Send + Sync>> {
    log::debug!("Rust: create_channel_and_header called. ca_path: {:?}", ca_path);
    let emulator_host = std::env::var("PUBSUB_EMULATOR_HOST").ok();
    let endpoint = emulator_host.as_ref().map(|h| format!("http://{}", h)).unwrap_or_else(|| "https://pubsub.googleapis.com".to_string());
    
    // Check pool first
    // Note: If ca_path varies, pooling by endpoint key alone is RISKY.
    // However, in Spark, all readers usually share same config.
    // Ideally key should include ca_path hash.
    // For now, I'll append ca_path to key if present?
    let pool_key = if let Some(p) = ca_path {
        format!("{}|{}", endpoint, p)
    } else {
        endpoint.clone()
    };
    
    let channel = {
        let pool = CONNECTION_POOL.lock().unwrap_or_else(|e| e.into_inner());
        pool.get(&pool_key).cloned()
    };

    let channel = if let Some(ch) = channel {
        ch
    } else {
        log::debug!("Rust: Creating new gRPC channel for {}", endpoint);
        let mut endpoint_builder = Channel::from_shared(endpoint.clone())?;
        if emulator_host.is_none() {
            let tls_config = if let Some(path) = ca_path {
                log::info!("Rust: Loading explicit CA roots from {}", path);
                let ca_data = std::fs::read(path).map_err(|e| format!("Failed to read CA file {}: {}", path, e))?;
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_data))
            } else {
                log::debug!("Rust: Using system native root certificates");
                ClientTlsConfig::new().with_native_roots()
            };
            endpoint_builder = endpoint_builder.tls_config(tls_config)?;
        }
        let ch = endpoint_builder.connect().await?;
        let mut pool = CONNECTION_POOL.lock().unwrap_or_else(|e| e.into_inner());
        pool.insert(pool_key, ch.clone());
        ch
    };

    let header_val = if emulator_host.is_some() {
        None
    } else {
        log::info!("Rust: Looking for Application Default Credentials (ADC)...");
        let config = google_cloud_auth::project::Config {
            scopes: Some(&[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/pubsub"
            ]),
            ..Default::default()
        };
        
        // Try to get from global cache first
        let ts = if let Some(ts) = GLOBAL_TOKEN_SOURCE.get() {
            ts
        } else {
            let mut retry_count = 0;
            let max_retries = 3;
            let mut last_error = None;
            
            let mut ts_final = None;
            while retry_count <= max_retries {
                match google_cloud_auth::token::DefaultTokenSourceProvider::new(config.clone()).await {
                    Ok(ts) => {
                        ts_final = Some(ts);
                        break;
                    }
                    Err(e) => {
                        log::warn!("Rust: Failed to create TokenSource (attempt {}): {:?}", retry_count + 1, e);
                        last_error = Some(e.into());
                    }
                }
                retry_count += 1;
                if retry_count <= max_retries {
                    let delay = Duration::from_millis(500 * (1 << retry_count));
                    tokio::time::sleep(delay).await;
                }
            }
            
            if let Some(ts) = ts_final {
                // Try to initialize GLOBAL_TOKEN_SOURCE, if someone else beat us to it, that's fine.
                let _ = GLOBAL_TOKEN_SOURCE.set(ts);
                GLOBAL_TOKEN_SOURCE.get().unwrap()
            } else {
                return Err(last_error.unwrap_or_else(|| "Unknown ADC error".into()));
            }
        };

        log::info!("Rust: ADC Loaded successfully.");
        let token_source = ts.token_source();
        let token = token_source.token().await.map_err(|e| format!("Token error: {}", e))?;
        
        let header_string = if token.starts_with("Bearer ") {
            token
        } else {
            format!("Bearer {}", token)
        };
        let val = MetadataValue::from_str(&header_string)?;
        Some(val)
    };

    Ok((channel, header_val))
}

enum WriterCommand {
    Publish(Vec<PubsubMessage>),
    Flush(tokio::sync::oneshot::Sender<()>),
}

pub struct PublisherClient {
    tx: tokio::sync::mpsc::Sender<WriterCommand>,
}

impl PublisherClient {
    pub async fn new(project_id: &str, topic_id: &str, ca_path: Option<&str>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (channel, header_val) = create_channel_and_header(ca_path).await?;
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
            log::debug!("Rust: Publisher background task started for topic: {}", topic);
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    WriterCommand::Publish(messages) => {
                         if messages.is_empty() { continue; }
                        
                        log::debug!("Rust: Publishing batch of {} messages to {}", messages.len(), topic);
                        
                        // INSTRUMENTATION - PRE-CALCULATE BEFORE MOVE
                        let batch_bytes: u64 = messages.iter().map(|m| m.data.len() as u64).sum();
                        let batch_count = messages.len() as u64;

                        let req = google_cloud_googleapis::pubsub::v1::PublishRequest {
                            topic: topic.clone(),
                            messages,
                        };
                        
                        let mut backoff_millis = 100;
                        let max_backoff = 60000; // 60s

                        loop {
                            let mut request = Request::new(req.clone());
                            if let Some(val) = &header_val_clone {
                                request.metadata_mut().insert("authorization", val.clone());
                            }
                            
                            let start = std::time::Instant::now();
                            match client.publish(request).await {
                                Ok(_) => {
                                    let duration = start.elapsed().as_micros() as u64;
                                    PUBLISH_LATENCY_TOTAL_MICROS.fetch_add(duration, Ordering::Relaxed);
                                    log::debug!("Rust: Publish batch successful");
                                    PUBLISHED_BYTES.fetch_add(batch_bytes, Ordering::Relaxed);
                                    PUBLISHED_MESSAGES.fetch_add(batch_count, Ordering::Relaxed);
                                    break;
                                }, 
                                Err(e) => {
                                    WRITE_ERRORS.fetch_add(1, Ordering::Relaxed);
                                    RETRY_COUNT.fetch_add(1, Ordering::Relaxed);
                                    // ... Error handling ...
                                    let code = e.code();
                                    if code == tonic::Code::NotFound || code == tonic::Code::PermissionDenied || code == tonic::Code::InvalidArgument {
                                        log::error!("Rust: Fatal Publish Error: {:?}. Stopping background task.", e);
                                        return; 
                                    }
                                    let mut actual_backoff = backoff_millis;
                                    if code == tonic::Code::ResourceExhausted {
                                        log::warn!("Rust: Quota exceeded (RESOURCE_EXHAUSTED). Using aggressive backoff.");
                                        actual_backoff *= 2; // Extra multiplier for quota issues
                                    }

                                    // Add jitter (±20%)
                                    let jitter = rand::Rng::gen_range(&mut rand::thread_rng(), 0.8..1.2);
                                    let sleep_ms = (actual_backoff as f64 * jitter) as u64;

                                    log::warn!("Rust: Async Publish failed: {:?}. Retrying in {}ms", e, sleep_ms);
                                    tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                                    backoff_millis = std::cmp::min(backoff_millis * 2, max_backoff);
                                }
                            }
                        }
                    },
                    WriterCommand::Flush(ack_tx) => {
                        let _ = ack_tx.send(());
                        log::info!("Rust: Flush completed.");
                    }
                }
            }
            log::info!("Rust: Publisher background task ended");
        });

        Ok(Self { tx })
    }
    
    pub async fn publish_batch(&mut self, messages: Vec<PubsubMessage>) -> Result<(), Box<dyn std::error::Error>> {
        if messages.is_empty() {
             return Ok(());
        }
        self.tx.send(WriterCommand::Publish(messages)).await.map_err(|e| format!("Failed to queue batch for publish: {}", e))?;
        Ok(())
    }
    
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
