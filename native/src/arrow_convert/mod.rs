//! # Arrow Conversion Logic
//!
//! This module handles the transformation between Google Cloud Pub/Sub messages
//! and Apache Arrow `RecordBatch` structures.
//!
//! - `builder.rs`: Handles reading Pub/Sub messages -> Arrow (supporting Raw and Structured JSON)
//! - `reader.rs`: Handles writing Arrow -> Pub/Sub messages (supporting Raw and Structured JSON)

pub mod builder;
pub mod reader;

pub use builder::ArrowBatchBuilder;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, StringArray};
    use google_cloud_googleapis::pubsub::v1::{PubsubMessage, ReceivedMessage};

    #[test]
    fn test_arrow_batch_builder_raw() {
        let mut builder = ArrowBatchBuilder::new(None);

        let msg1 = ReceivedMessage {
            ack_id: "ack1".to_string(),
            message: Some(PubsubMessage {
                data: b"payload1".to_vec(),
                attributes: Default::default(),
                message_id: "id1".to_string(),
                publish_time: Some(prost_types::Timestamp {
                    seconds: 1600000000,
                    nanos: 0,
                }),
                ordering_key: "".to_string(),
            }),
            delivery_attempt: 0,
        };

        builder.append(&msg1);

        let (arrays, schema) = builder.finish();

        assert_eq!(arrays.len(), 5); // message_id, publish_time, payload, ack_id, attributes
        let payload_idx = schema.index_of("payload").unwrap();
        let payload_array = arrays[payload_idx]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(payload_array.value(0), b"payload1");
    }
}
