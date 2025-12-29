//! State management for Pub/Sub message handles and Spark batch acknowledgments.
//!
//! This module provides global maps that preserve message leases (via `ACK_HANDLE_MAP`)
//! and track batch-to-message mapping (`BATCH_ACK_MAP`) to facilitate atomic commits
//! in the Spark Streaming lifecycle.

use dashmap::DashMap;
use google_cloud_pubsub::subscriber::ReceivedMessage as HighLevelMessage;
use once_cell::sync::Lazy;

/// Global map holding high-level message handles.
/// Handles MUST be held in memory for the duration of the lease to prevent automatic
/// redelivery by the Pub/Sub service before Spark acknowledges the batch.
/// Key: Message AckId, Value: ReceivedMessage handle.
pub static ACK_HANDLE_MAP: Lazy<DashMap<String, HighLevelMessage>> = Lazy::new(DashMap::new);

/// Map tracking which messages belong to a specific Spark batch.
/// This allows the connector to perform bulk acknowledgments when Spark commits the micro-batch.
/// Key: Partition-Batch Identifier (e.g., "p0-123"), Value: Vector of AckIds.
pub static BATCH_ACK_MAP: Lazy<DashMap<String, Vec<String>>> = Lazy::new(DashMap::new);

/// Purges all unacked messages and batch state for a partition.
/// This prevents memory leaks if Spark partitions are abandoned.
pub fn cleanup_partition(partition_id: i32, batch_id: Option<&str>) {
    log::info!("Rust: Cleaning up resources for partition {}", partition_id);

    if let Some(bid) = batch_id {
        // Purge specific batch
        let key = format!("p{}-{}", partition_id, bid);
        if let Some((_, ack_ids)) = BATCH_ACK_MAP.remove(&key) {
            for id in ack_ids {
                ACK_HANDLE_MAP.remove(&id);
            }
        }
    } else {
        // Purge ALL batches for this partition (called on close)
        let prefix = format!("p{}-", partition_id);
        let mut keys_to_remove = Vec::new();
        for entry in BATCH_ACK_MAP.iter() {
            if entry.key().starts_with(&prefix) {
                keys_to_remove.push(entry.key().clone());
            }
        }
        for k in keys_to_remove {
            if let Some((_, ack_ids)) = BATCH_ACK_MAP.remove(&k) {
                for id in ack_ids {
                    ACK_HANDLE_MAP.remove(&id);
                }
            }
        }
    }
}
