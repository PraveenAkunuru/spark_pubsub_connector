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

/// Client for interacting with Google Cloud Pub/Sub via gRPC.
/// 
/// This client uses `StreamingPull` to efficiently buffer messages in the background.
pub struct PubSubClient {
    /// Receiver for buffered messages from the background task.
    receiver: Receiver<PubsubMessage>,
}

/// Shared connection logic for Pub/Sub gRPC clients.
async fn create_channel_and_header() -> Result<(Channel, Option<MetadataValue<tonic::metadata::Ascii>>), Box<dyn std::error::Error + Send + Sync>> {
    // 2. Connect
    // Check for emulator
    let emulator_host = std::env::var("PUBSUB_EMULATOR_HOST").ok();
    
    if let Some(host) = emulator_host {
        let endpoint = format!("http://{}", host);
        let channel = Channel::from_shared(endpoint)?
            .connect()
            .await?;
        // No auth for emulator
        Ok((channel, None))
    } else {
        // 1. Auth (ADC)
        let auth_config = Config::default();
        let ts_provider = DefaultTokenSourceProvider::new(auth_config).await
            .map_err(|e| format!("Failed to create token source provider: {}", e))?;
        let ts = ts_provider.token_source();
        
        let channel = Channel::from_static("https://pubsub.googleapis.com")
            .connect()
            .await?;
            
        let token_val = ts.token().await
            .map_err(|e| format!("Failed to get access token: {}", e))?;
        let bearer_token = format!("Bearer {}", token_val);
        let header_val = MetadataValue::from_str(&bearer_token)?;
        Ok((channel, Some(header_val)))
    }
}

impl PubSubClient {
    pub async fn new(project_id: &str, subscription_id: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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
        tokio::spawn(async move {
            let (req_tx, req_stream) = tokio::sync::mpsc::channel(100);
            let request_stream = tokio_stream::wrappers::ReceiverStream::new(req_stream);
            
            // Send initial request
            let init_req = google_cloud_googleapis::pubsub::v1::StreamingPullRequest {
                subscription: full_sub_name.clone(),
                stream_ack_deadline_seconds: 10,
                ack_ids: vec![],
                modify_deadline_seconds: vec![],
                modify_deadline_ack_ids: vec![],
                client_id: "rust-spark-connector".to_string(),
                max_outstanding_messages: 1000,
                max_outstanding_bytes: 10 * 1024 * 1024,
            };
            
            if (req_tx.send(init_req).await).is_err() {
                return; // broken
            }
            
            // We keep `req_tx` alive if we need to send Acks later.
            
            let response_stream = client.streaming_pull(Request::new(request_stream)).await;
            
            match response_stream {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    while let Ok(Some(resp)) = stream.message().await {
                        for recv_msg in resp.received_messages {
                             if let Some(inner_msg) = recv_msg.message {
                                 if tx.send(inner_msg).await.is_err() {
                                     return;
                                 }
                             }
                        }
                    }
                },
                Err(e) => {
                    eprintln!("Rust: StreamingPull failed: {:?}", e);
                }
            }
        });

        Ok(Self {
            receiver: rx,
        })
    }

    pub async fn fetch_batch(&mut self, batch_size: usize) -> Vec<PubsubMessage> {
        eprintln!("Rust: fetch_batch called. Requesting max {} messages", batch_size);
        let mut batch = Vec::with_capacity(batch_size);
        while batch.len() < batch_size {
             // Use timeout to allow partial batches
             match tokio::time::timeout(Duration::from_millis(50), self.receiver.recv()).await {
                 Ok(Some(msg)) => batch.push(msg),
                 Ok(None) => break, // closed
                 Err(_) => break, // timeout
             }
        }
        batch
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
