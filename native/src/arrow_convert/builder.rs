use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int32Builder,
    Int64Builder, MapBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use google_cloud_googleapis::pubsub::v1::ReceivedMessage;
use serde_json::Value;
use std::sync::Arc;
use super::DataFormat;
use apache_avro::types::Value as AvroValue;

/// Builder for converting a stream of `ReceivedMessage` objects into an Arrow batch.
/// Each field (message_id, publish_time, payload, ack_id, attributes) is managed by its own columnar builder.
pub struct ArrowBatchBuilder {
    // Metadata Fields
    message_ids: StringBuilder,
    publish_times: TimestampMicrosecondBuilder,
    ack_ids: StringBuilder,
    attributes: MapBuilder<StringBuilder, StringBuilder>,

    // Data Fields
    is_raw: bool,
    payloads: Option<BinaryBuilder>, // Used if is_raw = true
    struct_builders: Option<Vec<Box<dyn ArrayBuilder>>>, // Used if is_raw = false (Schema projection)
    struct_fields: Vec<Field>,                           // Corresponds to struct_builders
    
    // Configuration
    format: DataFormat,
    avro_schema: Option<apache_avro::Schema>,
}

impl ArrowBatchBuilder {
    /// Creates a new builder. If schema is None, defaults to "Raw Mode" (payload as Binary).
    /// If schema is provided, attempts to project JSON/Avro payloads into that schema.
    pub fn new(
        schema: Option<SchemaRef>, 
        format: DataFormat,
        avro_schema: Option<apache_avro::Schema>
    ) -> Self {
        if let Some(s) = schema {
            // Structured Mode
            let allowed_fields: Vec<Field> = s
                .fields()
                .iter()
                .filter(|f| {
                    !["message_id", "publish_time", "ack_id", "attributes"]
                        .contains(&f.name().as_str())
                })
                .map(|f| f.as_ref().clone())
                .collect();

            let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
            for f in &allowed_fields {
                builders.push(arrow::array::make_builder(f.data_type(), 1024));
            }

            Self {
                message_ids: StringBuilder::new(),
                publish_times: TimestampMicrosecondBuilder::new(),
                ack_ids: StringBuilder::new(),
                attributes: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
                is_raw: false,
                payloads: None,
                struct_builders: Some(builders),
                struct_fields: allowed_fields,
                format,
                avro_schema,
            }
        } else {
            // Raw Mode
            Self {
                message_ids: StringBuilder::new(),
                publish_times: TimestampMicrosecondBuilder::new(),
                ack_ids: StringBuilder::new(),
                attributes: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
                is_raw: true,
                payloads: Some(BinaryBuilder::new()),
                struct_builders: None,
                struct_fields: Vec::new(),
                format: DataFormat::Json, // Raw mode implies binary payload, but format tracked
                avro_schema: None,
            }
        }
    }

    /// Appends a single `ReceivedMessage` (including its data and metadata) to the columnar builders.
    pub fn append(&mut self, recv_msg: &ReceivedMessage) {
        let msg = recv_msg
            .message
            .as_ref()
            .expect("ReceivedMessage must have a message");
        self.message_ids.append_value(&msg.message_id);

        let timestamp_micros = if let Some(ts) = &msg.publish_time {
            ts.seconds * 1_000_000 + (ts.nanos as i64 / 1_000)
        } else {
            0
        };
        self.publish_times.append_value(timestamp_micros);
        self.ack_ids.append_value(&recv_msg.ack_id);

        // Attributes
        for (k, v) in &msg.attributes {
            self.attributes.keys().append_value(k);
            self.attributes.values().append_value(v);
        }
        self.attributes
            .append(true)
            .expect("Failed to append attributes map");

        if self.is_raw {
            self.payloads.as_mut().unwrap().append_value(&msg.data);
        } else {
            // Structured Mode
            let builders = self.struct_builders.as_mut().unwrap();
            let fields = &self.struct_fields;

            match self.format {
                DataFormat::Json => {
                    let json_res = serde_json::from_slice::<Value>(&msg.data);
                    let json_val = json_res.as_ref().ok();
                    Self::append_json_to_row(builders, fields, json_val, &msg.attributes);
                }
                DataFormat::Avro => {
                    // Try parsing Avro
                    match &self.avro_schema {
                        Some(schema) => {
                             let avro_val = apache_avro::from_avro_datum(schema, &mut &msg.data[..], None).ok();
                             // We ignore errors for now (treat as null/missing) similar to JSON
                             Self::append_avro_to_row(builders, fields, avro_val.as_ref(), &msg.attributes);
                        }
                        None => {
                            // No schema? Cannot read avro. Append nulls.
                             Self::append_nulls_for_all(builders, fields);
                        }
                    }
                }
            }
        }
    }

    /// Appends a row from an Avro record.
    fn append_avro_to_row(
        builders: &mut [Box<dyn ArrayBuilder>],
        fields: &[Field],
        record: Option<&AvroValue>,
        attributes: &std::collections::HashMap<String, String>,
    ) {
        for (i, field) in fields.iter().enumerate() {
            let field_name = field.name();
            // Try to find field in Avro record
            let mut found_val: Option<&AvroValue> = None;

            if let Some(AvroValue::Record(entries)) = record {
                for (k, v) in entries {
                    if k == field_name {
                        found_val = Some(v);
                        break;
                    }
                }
            }

            // Use the value from Avro if present and not Null, otherwise fallback to attributes
            match found_val {
                Some(val) if !matches!(val, AvroValue::Null) => {
                    Self::append_avro_value(&mut builders[i], val, field.data_type());
                }
                _ => {
                    if let Some(attr_val) = attributes.get(field_name) {
                        Self::append_attr_value(&mut builders[i], attr_val, field.data_type());
                    } else {
                        Self::append_null(&mut builders[i], field.data_type());
                    }
                }
            }
        }
    }

    /// Appends a single Avro value to the given builder, performing type-safe conversion.
    fn append_avro_value(
        builder: &mut Box<dyn ArrayBuilder>,
        value: &AvroValue,
        dtype: &DataType
    ) {
        match dtype {
            DataType::Utf8 => {
                // Avro String or Bytes or logical types
                let s_opt = match value {
                    AvroValue::String(s) => Some(s.clone()),
                    AvroValue::Bytes(b) => String::from_utf8(b.clone()).ok(),
                    _ => Some(format!("{:?}", value)), // Fallback: debug string?
                };
                 builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_option(s_opt);
            }
            DataType::Int32 => {
                let i = match value {
                    AvroValue::Int(v) => Some(*v),
                    AvroValue::Long(v) => Some(*v as i32), // Truncate?
                    _ => None,
                };
                 builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .unwrap()
                    .append_option(i);
            }
            DataType::Int64 => {
                 let i = match value {
                    AvroValue::Long(v) => Some(*v),
                    AvroValue::Int(v) => Some(*v as i64),
                    _ => None,
                };
                 builder
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .unwrap()
                    .append_option(i);
            }
            DataType::Float64 => {
                 let f = match value {
                    AvroValue::Double(v) => Some(*v),
                    AvroValue::Float(v) => Some(*v as f64),
                    _ => None,
                };
                  builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .unwrap()
                    .append_option(f);
            }
             DataType::Boolean => {
                 let b = match value {
                    AvroValue::Boolean(v) => Some(*v),
                    _ => None,
                };
                 builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap()
                    .append_option(b);
            }
            _ => Self::append_null(builder, dtype),
        }
    }
    
    /// Appends null values to all provided builders.
    fn append_nulls_for_all(builders: &mut [Box<dyn ArrayBuilder>], fields: &[Field]) {
         for (i, field) in fields.iter().enumerate() {
             Self::append_null(&mut builders[i], field.data_type());
         }
    }

    /// Appends a row from a JSON value.
    fn append_json_to_row(
        builders: &mut [Box<dyn ArrayBuilder>],
        fields: &[Field],
        json: Option<&Value>,
        attributes: &std::collections::HashMap<String, String>,
    ) {
        for (i, field) in fields.iter().enumerate() {
            let field_name = field.name();
            let val = json.and_then(|j| j.get(field_name));
            
            // Try explicit JSON value first, then fallback to attributes
            if val.is_some() && !val.unwrap().is_null() {
                 Self::append_json_value(&mut builders[i], val, field.data_type());
            } else if let Some(attr_val) = attributes.get(field_name) {
                 // Attribute found! Convert string to target type.
                 Self::append_attr_value(&mut builders[i], attr_val, field.data_type());
            } else {
                 Self::append_null(&mut builders[i], field.data_type());
            }
        }
    }
    
    /// Appends an attribute value to a builder, attempting to parse it into the target type.
    fn append_attr_value(
        builder: &mut Box<dyn ArrayBuilder>,
        value: &str,
        dtype: &DataType,
    ) {
         match dtype {
            DataType::Utf8 => {
                 builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_value(value);
            }
            DataType::Int32 => {
                 let i = value.parse::<i32>().unwrap_or(0);
                 builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .unwrap()
                    .append_value(i);
            }
            DataType::Int64 => {
                 let i = value.parse::<i64>().unwrap_or(0);
                 builder
                    .as_any_mut()
                    .downcast_mut::<Int64Builder>()
                    .unwrap()
                    .append_value(i);
            }
             DataType::Float64 => {
                 let f = value.parse::<f64>().unwrap_or(0.0);
                 builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .unwrap()
                    .append_value(f);
            }
            DataType::Boolean => {
                 let b = value.parse::<bool>().unwrap_or(false);
                 builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap()
                    .append_value(b);
            }
            _ => {
                // Complex types from attributes not supported easily (need JSON parsing of attr string?)
                // For now, append null
                Self::append_null(builder, dtype);
            }
         }
    }

    /// Appends a single JSON value to its corresponding builder.
    fn append_json_value(
        builder: &mut Box<dyn ArrayBuilder>,
        value: Option<&Value>,
        dtype: &DataType,
    ) {
        // Helper to append JSON value to Arrow Builder
        match value {
            None | Some(Value::Null) => {
                Self::append_null(builder, dtype);
            }
            Some(v) => {
                match dtype {
                    DataType::Utf8 => {
                        let s = if v.is_string() {
                            v.as_str().unwrap()
                        } else {
                            &v.to_string()
                        };
                        builder
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap()
                            .append_value(s);
                    }
                    DataType::Int32 => {
                        let i = v.as_i64().unwrap_or(0) as i32;
                        builder
                            .as_any_mut()
                            .downcast_mut::<Int32Builder>()
                            .unwrap()
                            .append_value(i);
                    }
                    DataType::Int64 => {
                        let i = v.as_i64().unwrap_or(0);
                        builder
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap()
                            .append_value(i);
                    }
                    DataType::Float64 => {
                        let f = v.as_f64().unwrap_or(0.0);
                        builder
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap()
                            .append_value(f);
                    }
                    DataType::Boolean => {
                        let b = v.as_bool().unwrap_or(false);
                        builder
                            .as_any_mut()
                            .downcast_mut::<BooleanBuilder>()
                            .unwrap()
                            .append_value(b);
                    }
                    DataType::Struct(_sub_fields) => {
                         if let Some(sb_child) = builder.as_any_mut().downcast_mut::<StructBuilder>()
                        {
                            sb_child.append(false); // Null for nested struct
                        }
                    }
                    _ => {
                        // Unsupported: Append null
                        Self::append_null(builder, dtype);
                    }
                }
            }
        }
    }

    /// Appends a null value to the builder for the given data type.
    fn append_null(builder: &mut Box<dyn ArrayBuilder>, dtype: &DataType) {
        match dtype {
            DataType::Utf8 => builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_null(),
            DataType::Int32 => builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .unwrap()
                .append_null(),
            DataType::Int64 => builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_null(),
            DataType::Float64 => builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap()
                .append_null(),
            DataType::Boolean => builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_null(),
            DataType::Struct(_) => {
                if let Some(sb) = builder.as_any_mut().downcast_mut::<StructBuilder>() {
                     sb.append(false);
                }
            }
            _ => { /* Unsupported types just ignored or panic? Ignored for now. */ }
        }
    }

    /// Finalizes the builders and returns the Arrow arrays and corresponding schema.
    pub fn finish(&mut self) -> (Vec<ArrayRef>, SchemaRef) {
        let message_id_array = Arc::new(self.message_ids.finish()) as ArrayRef;
        let publish_time_array = Arc::new(self.publish_times.finish()) as ArrayRef;
        let ack_id_array = Arc::new(self.ack_ids.finish()) as ArrayRef;
        let attributes_array = Arc::new(self.attributes.finish()) as ArrayRef;

        // Define metadata fields
        let mut fields = vec![
            Field::new("message_id", DataType::Utf8, false),
            Field::new(
                "publish_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("ack_id", DataType::Utf8, false),
            Field::new(
                "attributes",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ];

        let mut arrays = vec![
            message_id_array,
            publish_time_array,
            ack_id_array,
            attributes_array,
        ];

        if self.is_raw {
            let payload_array = Arc::new(self.payloads.as_mut().unwrap().finish()) as ArrayRef;
            fields.insert(2, Field::new("payload", DataType::Binary, true));
            arrays.insert(2, payload_array);
        } else if let Some(builders) = self.struct_builders.as_mut() {
            for (i, builder) in builders.iter_mut().enumerate() {
                let arr = builder.finish();
                arrays.push(arr);
                fields.push(self.struct_fields[i].clone());
            }
        }

        let schema = Arc::new(Schema::new(fields));
        (arrays, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray};
    use google_cloud_googleapis::pubsub::v1::PubsubMessage;

    #[test]
    fn test_arrow_batch_builder_raw() {
        let mut builder = ArrowBatchBuilder::new(None, DataFormat::Json, None);

        let msg1 = ReceivedMessage {
            ack_id: "ack1".to_string(),
            message: Some(PubsubMessage {
                data: b"payload1".to_vec(),
                attributes: Default::default(),
                message_id: "id1".to_string(),
                publish_time: Some(prost_types::Timestamp {
                    seconds: 1_600_000_000,
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

    #[test]
    fn test_arrow_batch_builder_structured_with_attributes() {
        use arrow::datatypes::{DataType, Field};
        use std::collections::HashMap;

        // Schema: [name: Utf8, age: Int32, source: Utf8]
        // "source" will come from attributes
        let fields = vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new("source", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));

        let mut builder = ArrowBatchBuilder::new(Some(schema.clone()), DataFormat::Json, None);

        let mut attr = HashMap::new();
        attr.insert("source".to_string(), "pubsub_attribute".to_string());

        let msg1 = ReceivedMessage {
            ack_id: "ack1".to_string(),
            message: Some(PubsubMessage {
                data: serde_json::to_vec(&serde_json::json!({
                    "name": "Alice",
                    "age": 30
                })).unwrap(),
                attributes: attr,
                message_id: "id1".to_string(),
                publish_time: None,
                ordering_key: "".to_string(),
            }),
            delivery_attempt: 0,
        };

        builder.append(&msg1);
        let (arrays, _) = builder.finish();

        // 0: message_id, 1: publish_time, 2: ack_id, 3: attributes
        // 4: name, 5: age, 6: source
        let name_arr = arrays[4].as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let age_arr = arrays[5].as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        let source_arr = arrays[6].as_any().downcast_ref::<arrow::array::StringArray>().unwrap();

        assert_eq!(name_arr.value(0), "Alice");
        assert_eq!(age_arr.value(0), 30);
        assert_eq!(source_arr.value(0), "pubsub_attribute");
    }
}

