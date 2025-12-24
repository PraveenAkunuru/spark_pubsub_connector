use google_cloud_googleapis::pubsub::v1::subscriber_client::SubscriberClient;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_auth::project::Config;
use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_token::TokenSourceProvider;
use tonic::{transport::Channel, Request};
use tonic::metadata::MetadataValue;
use tokio::sync::mpsc::{self, Receiver};
use tokio::time::Duration;
use std::str::FromStr;

use google_cloud_googleapis::pubsub::v1::ReceivedMessage;

use google_cloud_googleapis::pubsub::v1::StreamingPullRequest;
use tokio::sync::mpsc::Sender;

/// Client for interacting with Google Cloud Pub/Sub via gRPC.
/// 
/// This client uses `StreamingPull` to efficiently buffer messages in the background.
pub struct PubSubClient {
    /// Receiver for buffered messages from the background task.
    receiver: Receiver<ReceivedMessage>,
    /// Sender for requests (Acks) to the background task.
    sender: Sender<StreamingPullRequest>,
}

// ... (skip create_channel_and_header)

impl PubSubClient {
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

        // Spawn background task for StreamingPull
        eprintln!("Rust: Spawning background task for StreamingPull");
        let (req_tx, req_stream) = tokio::sync::mpsc::channel(100);
        let req_tx_clone = req_tx.clone();
        
        tokio::spawn(async move {
            eprintln!("Rust: Background task started");
            let request_stream = tokio_stream::wrappers::ReceiverStream::new(req_stream);
            
            // Send initial request
            let init_req = StreamingPullRequest {
                subscription: full_sub_name.clone(),
                stream_ack_deadline_seconds: 10,
                ack_ids: vec![],
                modify_deadline_seconds: vec![],
                modify_deadline_ack_ids: vec![],
                client_id: "rust-spark-connector".to_string(),
                max_outstanding_messages: 1000,
                max_outstanding_bytes: 10 * 1024 * 1024,
            };
            
            // Use the clone inside the task? No, we created the channel here.
            // Wait, we need to return `req_tx` to PubSubClient.
            // So we can clone `req_tx` for the task if needed, or just let `client` take ownership of the original sender.
            // But `StreamingPull` takes `request_stream` (Receiver).
            // We need `Sender` in `PubSubClient`.
            
            // Let's send init request FIRST via sender?
            // If we use `ReceiverStream`, the sender controls the stream.
            // So `req_tx` IS the control handle.
            
            // We just need to ensure init_req is sent.
            if (req_tx_clone.send(init_req).await).is_err() {
                 eprintln!("Rust: Failed to send init request");
                 return; 
            }
            eprintln!("Rust: Init request sent. Waiting for response stream...");
            
            let response_stream = client.streaming_pull(Request::new(request_stream)).await;
            
            match response_stream {
                Ok(response) => {
                    eprintln!("Rust: StreamingPull response stream established");
                    let mut stream = response.into_inner();
                    while let Ok(Some(resp)) = stream.message().await {
                        for recv_msg in resp.received_messages {
                             if tx.send(recv_msg).await.is_err() {
                                 eprintln!("Rust: Failed to send message to internal channel. Receiver dropped?");
                                 return;
                             }
                        }
                    }
                    eprintln!("Rust: StreamingPull stream ended");
                },
                Err(e) => {
                    eprintln!("Rust: StreamingPull failed: {:?}", e);
                }
            }
        });

        Ok(Self {
            receiver: rx,
            sender: req_tx, 
        })
    }

    pub async fn fetch_batch(&mut self, batch_size: usize) -> Vec<ReceivedMessage> {
        // eprintln!("Rust: fetch_batch called. Requesting max {} messages", batch_size);
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
             match tokio::time::timeout(Duration::from_millis(3000), self.receiver.recv()).await {
                 Ok(Some(msg)) => batch.push(msg),
                 Ok(None) => break, // closed
                 Err(_) => break, // timeout
             }
        }
        batch
    }
    
    pub async fn acknowledge(&self, ack_ids: Vec<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if ack_ids.is_empty() {
            return Ok(());
        }
        let req = StreamingPullRequest {
            ack_ids,
            ..Default::default()
        };
        // We use the sender to send this request into the stream
        self.sender.send(req).await.map_err(|e| format!("Failed to send Ack request: {}", e))?;
        Ok(())
    }
}

/// Client for publishing messages to Google Cloud Pub/Sub via gRPC.
pub struct PublisherClient {
    client: google_cloud_googleapis::pubsub::v1::publisher_client::PublisherClient<Channel>,
    topic: String,
    auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl PublisherClient {
    pub async fn new(project_id: &str, topic_id: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (channel, header_val) = create_channel_and_header().await?;

        let client = google_cloud_googleapis::pubsub::v1::publisher_client::PublisherClient::new(channel);
        
        let full_topic_name = if topic_id.contains('/') {
            topic_id.to_string()
        } else {
            format!("projects/{}/topics/{}", project_id, topic_id)
        };

        Ok(Self {
            client,
            topic: full_topic_name,
            auth_header: header_val,
        })
    }
    
    pub async fn publish_batch(&mut self, messages: Vec<PubsubMessage>) -> Result<(), Box<dyn std::error::Error>> {
        if messages.is_empty() {
             return Ok(());
        }
        
        let req = google_cloud_googleapis::pubsub::v1::PublishRequest {
            topic: self.topic.clone(),
            messages,
        };
        
        let mut request = Request::new(req);
        if let Some(val) = &self.auth_header {
            request.metadata_mut().insert("authorization", val.clone());
        }
        
        self.client.publish(request).await?;
        Ok(())
    }
    
    pub async fn flush(&self) {
        // No-op for direct gRPC
    }
}
