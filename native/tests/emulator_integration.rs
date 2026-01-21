use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig; // Import this
use native_pubsub_connector::sink::PublisherClient;
use std::env;
use tokio::time::Duration;

#[tokio::test]
async fn test_emulator_end_to_end() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = env_logger::try_init();

    let emulator_host = env::var("PUBSUB_EMULATOR_HOST");
    if emulator_host.is_err() {
        println!("Skipping emulator test: PUBSUB_EMULATOR_HOST not set");
        return Ok(());
    }

    let project_id = "test-project";
    let topic_id = "integration-topic";
    let sub_id = "integration-sub";

    // 1. Create Topic and Sub
    let mut config = ClientConfig::default().with_auth().await?;
    config.project_id = Some(project_id.to_string());
    let client = Client::new(config).await?;
    let topic = client.topic(topic_id);

    if !topic.exists(None).await? {
        topic.create(None, None).await?;
    }

    let sub = client.subscription(sub_id);
    if !sub.exists(None).await? {
        sub.create(
            topic.fully_qualified_name(),
            SubscriptionConfig::default(),
            None,
        )
        .await?;
    }

    // 2. Publish Messages via PublisherClient
    let mut publisher = PublisherClient::new(
        project_id,
        topic_id,
        None,
        Some(10), // Batch size 10
        None,
        Some(100), // 100ms
    )
    .await?;

    let messages: Vec<PubsubMessage> = (0..20)
        .map(|i| PubsubMessage {
            data: format!("msg-{}", i).into_bytes(),
            ..Default::default()
        })
        .collect();

    publisher.publish_batch(messages).await?;
    publisher.flush(Duration::from_secs(5)).await?;

    // 3. Verify messages received
    let pulled = sub.pull(20, None).await?;
    assert_eq!(pulled.len(), 20, "Should receive 20 messages");

    // Ack them
    let ack_ids: Vec<String> = pulled.iter().map(|m| m.ack_id().to_string()).collect();
    sub.ack(ack_ids).await?;

    println!("Successfully verified 20 messages via Emulator!");

    // Cleanup
    sub.delete(None).await?;
    topic.delete(None).await?;

    Ok(())
}
