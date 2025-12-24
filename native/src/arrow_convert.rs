//! # Arrow Conversion Logic
//!
//! This module handles the transformation between Google Cloud Pub/Sub messages
//! and Apache Arrow `RecordBatch` structures.
//!
//! Why this module exists:
//! - **Performance**: Arrow is a columnar memory format that allows Spark to process data extremely efficiently.
//! - **Standardization**: By converting Pub/Sub messages to Arrow in native code, we provide a structured,
//!   binary-compatible format for Spark without JVM overhead for parsing.
//! - **Schema Mapping**: Defines how Pub/Sub metadata (message ID, publish time) is mapped to Arrow fields.

use arrow::array::{ArrayRef, BinaryBuilder, StringBuilder, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use std::sync::Arc;

use google_cloud_googleapis::pubsub::v1::ReceivedMessage;

/// Builder for converting a stream of `ReceivedMessage` objects into an Arrow batch.
/// Each field (message_id, publish_time, payload, ack_id) is managed by its own columnar builder.
pub struct ArrowBatchBuilder {
    message_ids: StringBuilder,
    publish_times: TimestampMicrosecondBuilder,
    payloads: BinaryBuilder,
    ack_ids: StringBuilder,
    // TODO: Add support for attributes (Map/List)
}

impl ArrowBatchBuilder {
    pub fn new() -> Self {
        Self {
            message_ids: StringBuilder::new(),
            publish_times: TimestampMicrosecondBuilder::new(),
            payloads: BinaryBuilder::new(),
            ack_ids: StringBuilder::new(),
        }
    }
    
    /// Appends a single `ReceivedMessage` (including its data and metadata) to the columnar builders.
    pub fn append(&mut self, recv_msg: &ReceivedMessage) {
        let msg = recv_msg.message.as_ref().expect("ReceivedMessage must have a message");
        self.message_ids.append_value(&msg.message_id);
        
        let timestamp_micros = if let Some(ts) = &msg.publish_time {
            ts.seconds * 1_000_000 + (ts.nanos as i64 / 1_000)
        } else {
            0 // Or null?
        };
        self.publish_times.append_value(timestamp_micros);
        
        self.payloads.append_value(&msg.data);
        self.ack_ids.append_value(&recv_msg.ack_id);
        
        // Attributes TODO
    }

    /// Finalizes the builders and returns the Arrow arrays and corresponding schema.
    pub fn finish(&mut self) -> (Vec<ArrayRef>, SchemaRef) {
        let message_id_array = Arc::new(self.message_ids.finish()) as ArrayRef;
        let publish_time_array = Arc::new(self.publish_times.finish()) as ArrayRef;
        let payload_array = Arc::new(self.payloads.finish()) as ArrayRef;
        let ack_id_array = Arc::new(self.ack_ids.finish()) as ArrayRef;
        
        let schema = Schema::new(vec![
            Field::new("message_id", DataType::Utf8, false),
            Field::new("publish_time", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("payload", DataType::Binary, false),
            Field::new("ack_id", DataType::Utf8, false),
        ]);
        
        (
            vec![message_id_array, publish_time_array, payload_array, ack_id_array],
            Arc::new(schema)
        )
    }
}

use arrow::array::{StructArray, BinaryArray, StringArray, BooleanArray, Int32Array, Int64Array, Float32Array, Float64Array, TimestampMicrosecondArray, Array};
use std::collections::HashMap;

/// Reader for converting Arrow `StructArray`s back into `PubsubMessage` objects for publishing.
/// It dynamically maps Arrow columns to Pub/Sub message fields and attributes.
pub struct ArrowBatchReader<'a> {
    array: &'a StructArray,
}

impl<'a> ArrowBatchReader<'a> {
    pub fn new(array: &'a StructArray) -> Self {
        Self { array }
    }
    
    /// Converts the entire Arrow batch into a vector of `PubsubMessage`s.
    /// Columns not matching core fields (payload, message_id, etc.) are converted to message attributes.
    pub fn to_pubsub_messages(&self) -> Result<Vec<PubsubMessage>, Box<dyn std::error::Error>> {
        let num_rows = self.array.len();
        let mut messages = Vec::with_capacity(num_rows);
        
        // Identify core columns
        let payload_col = self.array.column_by_name("payload")
            .ok_or_else(|| {
                let available_fields: Vec<_> = self.array.fields().iter().map(|f| f.name()).collect();
                format!("Missing 'payload' column in Arrow batch. Available fields: {:?}", available_fields)
            })?;
            
        let payload_binary = payload_col.as_any().downcast_ref::<BinaryArray>()
            .ok_or_else(|| format!("'payload' column is not Binary (found {:?})", payload_col.data_type()))?;

        let ordering_key_col = self.array.column_by_name("ordering_key");
        
        // Identify attribute columns (all others except core ones)
        let core_fields = ["payload", "message_id", "publish_time", "ordering_key"];
        let mut attr_indices = Vec::new();
        for (idx, field) in self.array.fields().iter().enumerate() {
            if !core_fields.contains(&field.name().as_str()) {
                attr_indices.push((idx, field.name().clone()));
            }
        }
        
        for i in 0..num_rows {
             if payload_binary.is_null(i) {
                 continue; // Skip null payloads
             }
             let data = payload_binary.value(i).to_vec();
             
             let mut attributes = HashMap::new();
             for (idx, name) in &attr_indices {
                 let col = self.array.column(*idx);
                 if !col.is_null(i) {
                     let val_str = match col.data_type() {
                         DataType::Utf8 => col.as_any().downcast_ref::<StringArray>().unwrap().value(i).to_string(),
                         DataType::Binary => format!("{:?}", col.as_any().downcast_ref::<BinaryArray>().unwrap().value(i)),
                         DataType::Boolean => col.as_any().downcast_ref::<BooleanArray>().unwrap().value(i).to_string(),
                         DataType::Int32 => col.as_any().downcast_ref::<Int32Array>().unwrap().value(i).to_string(),
                         DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().unwrap().value(i).to_string(),
                         DataType::Float32 => col.as_any().downcast_ref::<Float32Array>().unwrap().value(i).to_string(),
                         DataType::Float64 => col.as_any().downcast_ref::<Float64Array>().unwrap().value(i).to_string(),
                         DataType::Timestamp(_, _) => {
                              // Rough string representation
                              let micros = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(i);
                              format!("{}", micros)
                         }
                         _ => format!("{:?}", col.as_any()), // Fallback for other types
                     };
                     attributes.insert(name.clone(), val_str);
                 }
             }

             let ordering_key = if let Some(col) = ordering_key_col {
                 if !col.is_null(i) && col.data_type() == &DataType::Utf8 {
                     col.as_any().downcast_ref::<StringArray>().unwrap().value(i).to_string()
                 } else {
                     "".to_string()
                 }
             } else {
                 "".to_string()
             };
             
             messages.push(PubsubMessage {
                 data,
                 attributes,
                 message_id: "".to_string(),
                 publish_time: None,
                 ordering_key,
             });
        }
        
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_googleapis::pubsub::v1::PubsubMessage;
    use arrow::array::{StringArray, TimestampMicrosecondArray, BinaryArray, Array};

    #[test]
    fn test_arrow_batch_builder() {
        let mut builder = ArrowBatchBuilder::new();
        
        let msg1 = ReceivedMessage {
            ack_id: "ack1".to_string(),
            message: Some(PubsubMessage {
                data: b"payload1".to_vec(),
                attributes: Default::default(),
                message_id: "id1".to_string(),
                publish_time: Some(prost_types::Timestamp { seconds: 1600000000, nanos: 0 }),
                ordering_key: "".to_string(),
            }),
            delivery_attempt: 0,
        };
        
        builder.append(&msg1);
        
        let (arrays, schema) = builder.finish();
        
        assert_eq!(arrays.len(), 4); // message_id, publish_time, payload, ack_id
        assert_eq!(schema.fields().len(), 4);
        
        let id_array = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_array.value(0), "id1");
        
        let ts_array = arrays[1].as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(ts_array.value(0), 1_600_000_000_000_000);
        
        let payload_array = arrays[2].as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(payload_array.value(0), b"payload1");

        let ack_array = arrays[3].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ack_array.value(0), "ack1");
    }
}
