use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use std::env;
use native_pubsub_connector::pubsub::PubSubClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    
    let project_id = "test-project";
    let topic_id = "test-topic";
    let sub_id = "test-sub";
    
    // Ensure emulator host is set
    if env::var("PUBSUB_EMULATOR_HOST").is_err() {
        // Default locally
        env::set_var("PUBSUB_EMULATOR_HOST", "localhost:8085"); 
    }
    
    log::info!("Connecting to Emulator at {}", env::var("PUBSUB_EMULATOR_HOST").unwrap());

    // Setup: Create Client, Topic, Subscription
    let config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config).await.unwrap();
    
    let topic = client.topic(topic_id);
    if !topic.exists(None).await.unwrap() {
        topic.create(None, Default::default()).await.unwrap();
    }
    
    let subscription = client.subscription(sub_id);
    if !subscription.exists(None).await.unwrap() {
        subscription.create(topic.fully_qualified_name(), Default::default(), None).await.unwrap();
    }
    
    // Publish 10 messages
    let publisher = topic.new_publisher(None);
    for i in 0..10 {
        publisher.publish(PubsubMessage {
            data: format!("Message {}", i).into_bytes(),
            ..Default::default()
        }).await.get().await.unwrap();
    }
    
    log::info!("Published 10 messages");
    
    // Use our wrapper wrapper
    let spark_client = PubSubClient::new(project_id, sub_id, None).await?;
    
    log::info!("Fetching batch...");
    let messages = spark_client.fetch_batch(100, 5000).await?;
    log::info!("Fetched {} messages", messages.len());
    
    assert!(!messages.is_empty());
    
    // Ack them
    let ack_ids: Vec<String> = messages.iter().map(|m| m.ack_id.clone()).collect();
    spark_client.acknowledge(ack_ids).await?;
    log::info!("Acked messages");
    
    Ok(())
}
