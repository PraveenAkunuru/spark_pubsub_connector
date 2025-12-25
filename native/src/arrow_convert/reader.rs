use arrow::array::{Array, BinaryArray, StructArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

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
    /// - If 'payload' column exists: Uses strict mode (payload=data, others=attributes).
    /// - If 'payload' missing: Serializes row to JSON (excluding 'ordering_key').
    ///
    /// TODO: Support serialization to Avro or Protobuf payloads.
    pub fn to_pubsub_messages(&self) -> Result<Vec<PubsubMessage>, Box<dyn std::error::Error>> {
        let num_rows = self.array.len();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        let payload_col = self.array.column_by_name("payload");

        if let Some(payload_col) = payload_col {
            // === RAW MODE ===
            // Existing logic: payload -> data, others -> attributes
            let payload_binary = payload_col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "'payload' column must be Binary, found {:?}",
                        payload_col.data_type()
                    )
                })?;

            let ordering_key_col = self.array.column_by_name("ordering_key");

            // Identify attribute columns (all others except core ones)
            let core_fields = ["payload", "message_id", "publish_time", "ordering_key"];
            let mut attr_indices = Vec::new();
            for (idx, field) in self.array.fields().iter().enumerate() {
                if !core_fields.contains(&field.name().as_str()) {
                    attr_indices.push((idx, field.name().clone()));
                }
            }

            let mut messages = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                if payload_binary.is_null(i) {
                    // Empty message? Or skip? Let's produce empty data
                    // Note: Pub/Sub allows empty data if attributes exist.
                }
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
            // No payload column -> Serialize row to JSON
            // 1. Create RecordBatch
            let fields: Vec<Field> = self
                .array
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let columns = self.array.columns().to_vec();
            let batch = RecordBatch::try_new(schema, columns)?;

            // 2. Convert to JSON Rows using LineDelimitedWriter
            let mut buf = Vec::new();
            {
                let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
                writer
                    .write(&batch)
                    .map_err(|e| format!("Failed to write JSON: {}", e))?;
                writer
                    .finish()
                    .map_err(|e| format!("Failed to finish JSON writer: {}", e))?;
            }

            let json_str = String::from_utf8(buf).map_err(|e| format!("Invalid UTF-8: {}", e))?;

            let mut messages = Vec::with_capacity(num_rows);

            for line in json_str.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                let mut row: serde_json::Map<String, Value> = serde_json::from_str(line)
                    .map_err(|e| format!("Failed to parse JSON line: {}", e))?;

                // Extract metadata
                let ordering_key = if let Some(val) = row.remove("ordering_key") {
                    val.as_str().unwrap_or("").to_string()
                } else {
                    "".to_string()
                };

                // Extract explicit attributes (Map<String, String>)
                let mut attributes = HashMap::new();
                if let Some(attr_val) = row.remove("attributes") {
                    if let Value::Object(map) = attr_val {
                        for (k, v) in map {
                            if let Some(s) = v.as_str() {
                                attributes.insert(k, s.to_string());
                            } else {
                                attributes.insert(k, v.to_string());
                            }
                        }
                    }
                }

                // Remove other reserved read-only metadata
                row.remove("message_id");
                row.remove("publish_time");
                row.remove("ack_id");

                // Serialize remainder as data
                let data = serde_json::to_vec(&Value::Object(row)).unwrap_or_default();

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
}
