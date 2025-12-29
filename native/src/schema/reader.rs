//! # Arrow to Pub/Sub Message Reader
//!
//! This module implements the transformation of columnar Arrow `StructArray`s
//! back into `PubsubMessage` objects for publishing to Google Cloud Pub/Sub.
//!
//! ## Operational Modes
//!
//! 1. **Raw Mode**: If the Arrow array contains a `payload` column of type `Binary`,
//!    this column is used directly as the message data. Other columns (excluding
//!    metadata like `message_id`) are mapped to Pub/Sub attributes.
//!
//! 2. **Structured Mode**: If no `payload` column is present, the entire row
//!    is serialized into a line-delimited JSON string and used as the message data.

use arrow::array::{Array, BinaryArray, StructArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use std::collections::HashMap;
use std::sync::Arc;

/// Reader for converting Arrow `StructArray`s back into `PubsubMessage` objects.
pub struct ArrowBatchReader<'a> {
    /// The underlying Arrow StructArray containing the batch data.
    array: &'a StructArray,
}

impl<'a> ArrowBatchReader<'a> {
    /// Creates a new ArrowBatchReader for the given StructArray.
    pub fn new(array: &'a StructArray) -> Self {
        Self { array }
    }

    /// Converts the entire Arrow batch into a vector of `PubsubMessage`s.
    ///
    /// This method automatically detects the operational mode based on the 
    /// presence of a 'payload' column.
    pub fn to_pubsub_messages(&self) -> Result<Vec<PubsubMessage>, Box<dyn std::error::Error>> {
        let num_rows = self.array.len();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        let payload_col = self.array.column_by_name("payload");

        if let Some(col) = payload_col {
            // === RAW MODE ===
            let payload_binary = col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "'payload' column must be Binary, found {:?}",
                        col.data_type()
                    )
                })?;

            let ordering_key_col = self.array.column_by_name("ordering_key");

            // Identify attribute columns (all others except core/metadata ones)
            let core_fields = ["payload", "message_id", "publish_time", "ordering_key"];
            let mut attr_indices = Vec::new();
            for (idx, field) in self.array.fields().iter().enumerate() {
                if !core_fields.contains(&field.name().as_str()) {
                    attr_indices.push((idx, field.name().clone()));
                }
            }

            let mut messages = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let data = if payload_binary.is_null(i) {
                    Vec::new()
                } else {
                    payload_binary.value(i).to_vec()
                };

                let mut attributes = HashMap::new();
                for (idx, name) in &attr_indices {
                    let col = self.array.column(*idx);
                    if !col.is_null(i) {
                        let val_str =
                            arrow::util::display::array_value_to_string(col, i).unwrap_or_default();
                        attributes.insert(name.clone(), val_str);
                    }
                }

                let ordering_key = if let Some(col) = ordering_key_col {
                    if !col.is_null(i) {
                        arrow::util::display::array_value_to_string(col, i).unwrap_or_default()
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
        } else {
            // === STRUCTURED MODE ===
            // Identify metadata columns to exclude from the data payload
            let reserved_fields = ["message_id", "publish_time", "ack_id", "ordering_key", "attributes"];
            let mut data_indices = Vec::new();
            let mut data_fields = Vec::new();
            
            for (idx, field) in self.array.fields().iter().enumerate() {
                if !reserved_fields.contains(&field.name().as_str()) {
                    data_indices.push(idx);
                    data_fields.push(field.as_ref().clone());
                }
            }

            // Project the batch to data-only columns for JSON serialization
            let data_columns: Vec<arrow::array::ArrayRef> = data_indices.iter().map(|&i| self.array.column(i).clone()).collect();
            let data_schema = Arc::new(Schema::new(data_fields));
            let data_batch = RecordBatch::try_new(data_schema, data_columns)?;
            
            let mut json_buf = Vec::new();
            {
                let mut writer = arrow::json::LineDelimitedWriter::new(&mut json_buf);
                writer.write(&data_batch)?;
                writer.finish()?;
            }

            let ordering_key_col = self.array.column_by_name("ordering_key");
            
            let mut messages = Vec::with_capacity(num_rows);
            let mut lines = json_buf.split(|&b| b == b'\n');

            for i in 0..num_rows {
                let data = lines.next().unwrap_or(&[]).to_vec();
                
                let ordering_key = if let Some(col) = ordering_key_col {
                    if !col.is_null(i) {
                        arrow::util::display::array_value_to_string(col, i).unwrap_or_default()
                    } else {
                        "".to_string()
                    }
                } else {
                    "".to_string()
                };

                messages.push(PubsubMessage {
                    data,
                    attributes: HashMap::new(),
                    message_id: "".to_string(),
                    publish_time: None,
                    ordering_key,
                });
            }

            Ok(messages)
        }
    }
}
