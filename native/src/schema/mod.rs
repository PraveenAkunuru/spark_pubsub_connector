//! # Arrow Conversion Logic
//!
//! This module handles the transformation between Google Cloud Pub/Sub messages
//! and Apache Arrow `RecordBatch` structures.
//!
//! - `builder.rs`: Handles reading Pub/Sub messages -> Arrow (supporting Raw and Structured JSON)
//! - `reader.rs`: Handles writing Arrow -> Pub/Sub messages (supporting Raw and Structured JSON)

pub mod builder;
pub mod reader;

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
// use crate::arrow_convert::builder::ArrowBatchBuilder; // Removed duplicate

#[derive(serde::Deserialize, Debug)]
pub struct SimpleField {
    #[allow(dead_code)] // Used by serde for deserialization
    name: String,
    #[serde(rename = "type")]
    #[allow(dead_code)] // Used by serde for deserialization
    type_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    #[default]
    Json,
    Avro,
    Protobuf,
}

#[derive(Default)]
pub struct ProcessingConfig {
    pub arrow_schema: Option<Arc<Schema>>,
    pub format: DataFormat,
    pub avro_schema: Option<apache_avro::Schema>,
    pub protobuf_descriptor: Option<String>,
    pub protobuf_message_name: Option<String>,
    pub ca_certificate_path: Option<String>,
    pub batch_size: Option<usize>,
    pub batch_bytes: Option<usize>,
    pub batch_duration_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
struct ConfigDto {
    #[allow(dead_code)]
    columns: Option<Vec<SimpleField>>,
    #[allow(dead_code)]
    format: Option<DataFormat>,
    #[serde(rename = "avroSchema")]
    #[allow(dead_code)]
    avro_schema: Option<String>,
    #[serde(rename = "protobufDescriptor")]
    #[allow(dead_code)]
    protobuf_descriptor: Option<String>,
    #[serde(rename = "protobufMessageName")]
    #[allow(dead_code)]
    protobuf_message_name: Option<String>,
    #[serde(rename = "caCertificatePath")]
    #[allow(dead_code)]
    ca_certificate_path: Option<String>,
    #[serde(rename = "batchSize")]
    batch_size: Option<usize>,
    #[serde(rename = "batchBytes")]
    batch_bytes: Option<usize>,
    #[serde(rename = "batchDurationMs")]
    batch_duration_ms: Option<u64>,
}

pub fn parse_processing_config(json: &str) -> Result<ProcessingConfig, String> {
    // Try to parse as ConfigDto
    let config: ConfigDto = serde_json::from_str(json).map_err(|e| e.to_string())?;

    let format = config.format.unwrap_or(DataFormat::Json);

    let arrow_schema = if let Some(cols) = config.columns {
        let arrow_fields: Vec<Field> = cols
            .into_iter()
            .map(|f| {
                let dtype = match f.type_name.as_str() {
                    "string" => DataType::Utf8,
                    "int" => DataType::Int32,
                    "long" => DataType::Int64,
                    "boolean" => DataType::Boolean,
                    "float" => DataType::Float32,
                    "double" => DataType::Float64,
                    _ => DataType::Utf8,
                };
                Field::new(f.name, dtype, true)
            })
            .collect();
        Some(Arc::new(Schema::new(arrow_fields)))
    } else {
        None
    };

    let avro_schema = if let Some(s) = config.avro_schema {
        if format == DataFormat::Avro {
            Some(
                apache_avro::Schema::parse_str(&s)
                    .map_err(|e| format!("Invalid Avro Schema: {}", e))?,
            )
        } else {
            None
        }
    } else {
        None
    };

    Ok(ProcessingConfig {
        arrow_schema,
        format,
        avro_schema,
        protobuf_descriptor: config.protobuf_descriptor,
        protobuf_message_name: config.protobuf_message_name,
        ca_certificate_path: config.ca_certificate_path,
        batch_size: config.batch_size,
        batch_bytes: config.batch_bytes,
        batch_duration_ms: config.batch_duration_ms,
    })
}

/// Parses a simple list of fields into an Arrow Schema (Legacy/Pre-Config support)
pub fn parse_simple_schema(json: &str) -> Option<Arc<Schema>> {
    if let Ok(fields) = serde_json::from_str::<Vec<SimpleField>>(json) {
        let arrow_fields: Vec<Field> = fields
            .into_iter()
            .map(|f| {
                let dtype = match f.type_name.as_str() {
                    "string" => DataType::Utf8,
                    "int" => DataType::Int32,
                    "long" => DataType::Int64,
                    "boolean" => DataType::Boolean,
                    "float" => DataType::Float32,
                    "double" => DataType::Float64,
                    _ => DataType::Utf8,
                };
                Field::new(f.name, dtype, true)
            })
            .collect();
        return Some(Arc::new(Schema::new(arrow_fields)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_processing_config_defaults() {
        let json = "{}";
        let config = parse_processing_config(json).expect("Failed to parse empty config");
        assert_eq!(config.format, DataFormat::Json);
        assert!(config.arrow_schema.is_none());
    }

    #[test]
    fn test_parse_processing_config_with_columns() {
        let json = r#"{
            "columns": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "int"}
            ],
            "format": "json"
        }"#;
        let config = parse_processing_config(json).expect("Failed to parse config with columns");
        assert_eq!(config.format, DataFormat::Json);
        assert!(config.arrow_schema.is_some());
        
        let schema = config.arrow_schema.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "col1");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), "col2");
        assert_eq!(schema.field(1).data_type(), &DataType::Int32);
    }
    
    #[test]
    fn test_parse_processing_config_avro() {
        let json = r#"{
            "format": "avro",
            "avroSchema": "{\"type\":\"record\",\"name\":\"test\",\"fields\":[]}"
        }"#;
        let config = parse_processing_config(json).expect("Failed to parse avro config");
        assert_eq!(config.format, DataFormat::Avro);
        assert!(config.avro_schema.is_some());
    }
}
