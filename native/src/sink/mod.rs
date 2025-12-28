use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use tokio::time::{Duration, Instant};
use std::sync::atomic::Ordering;

use crate::core::metrics::{PUBLISHED_BYTES, PUBLISHED_MESSAGES, WRITE_ERRORS, PUBLISH_LATENCY_TOTAL_MICROS};

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
    
    pub async fn publish_batch(&mut self, messages: Vec<PubsubMessage>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut awaiters = Vec::with_capacity(messages.len());
        let start = Instant::now();
        
        for msg in messages {
            let size = msg.data.len() as u64;
            PUBLISHED_BYTES.fetch_add(size, Ordering::Relaxed);
            PUBLISHED_MESSAGES.fetch_add(1, Ordering::Relaxed);
            
            // publish() returns a Future<Awaiter>. We await the first future to get the Awaiter.
            let awaiter = self.publisher.publish(msg).await;
            awaiters.push(awaiter.get());
        }
        
        // Wait for all Awaiters to complete.
        let results = futures::future::join_all(awaiters).await;
        
        for res in results {
            if let Err(e) = res {
                log::error!("Rust: Publish error: {:?}", e);
                WRITE_ERRORS.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        PUBLISH_LATENCY_TOTAL_MICROS.fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        Ok(())
    }
    
    pub async fn flush(&self, _timeout: Duration) -> Result<(), String> {
        Ok(())
    }
}
