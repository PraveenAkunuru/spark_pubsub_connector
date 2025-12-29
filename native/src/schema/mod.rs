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
}

#[derive(Default)]
pub struct ProcessingConfig {
    pub arrow_schema: Option<Arc<Schema>>,
    pub format: DataFormat,
    pub avro_schema: Option<apache_avro::Schema>,
    pub ca_certificate_path: Option<String>,
}

#[derive(serde::Deserialize)]
struct ConfigDto {
    #[allow(dead_code)] // Used by serde for deserialization
    columns: Option<Vec<SimpleField>>,
    #[allow(dead_code)] // Used by serde for deserialization
    format: Option<DataFormat>,
    #[serde(rename = "avroSchema")]
    #[allow(dead_code)] // Used by serde for deserialization
    avro_schema: Option<String>,
    #[serde(rename = "caCertificatePath")]
    #[allow(dead_code)] // Used by serde for deserialization
    ca_certificate_path: Option<String>,
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
            Some(apache_avro::Schema::parse_str(&s).map_err(|e| format!("Invalid Avro Schema: {}", e))?)
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
        ca_certificate_path: config.ca_certificate_path,
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
