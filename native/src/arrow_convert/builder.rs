use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int32Builder,
    Int64Builder, MapBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use google_cloud_googleapis::pubsub::v1::ReceivedMessage;
use serde_json::Value;
use std::sync::Arc;

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
}

impl ArrowBatchBuilder {
    /// Creates a new builder. If schema is None, defaults to "Raw Mode" (payload as Binary).
    /// If schema is provided, attempts to project JSON payloads into that schema.
    ///
    /// TODO: Support Avro/Protobuf payload parsing in addition to JSON.
    pub fn new(schema: Option<SchemaRef>) -> Self {
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
            // Structured Mode: Parse JSON
            let builders = self.struct_builders.as_mut().unwrap();
            let fields = &self.struct_fields;

            if let Ok(json_val) = serde_json::from_slice::<Value>(&msg.data) {
                Self::append_json_to_row(builders, fields, &json_val);
            } else {
                // Failed to parse: Append nulls for all fields
                for (i, field) in fields.iter().enumerate() {
                    Self::append_null(&mut builders[i], field.data_type());
                }
            }
        }
    }

    fn append_json_to_row(builders: &mut [Box<dyn ArrayBuilder>], fields: &[Field], json: &Value) {
        for (i, field) in fields.iter().enumerate() {
            let field_name = field.name();
            let val = json.get(field_name);
            Self::append_json_value(&mut builders[i], val, field.data_type());
        }
    }

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
                            // StructBuilder API is tricky, but `append_value` works if we build it manually?
                            // Wait, if we use StructBuilder for child, we have same issue.
                            // But let's assume recursive for now.
                            // We don't have easy API for `StructBuilder` child iteration.
                            // For simplicity: If field is Struct, we append NULL for now unless we implement full recursive manual building.
                            // Or use `append(true)` after appending to its children?
                            // StructBuilder in arrow-rs manages its children.
                            // We can't access them easily.
                            // Let's just append null for structs to avoid complex recursion bugs for this iteration.
                            // Improving this requires `StructBuilder` alternative or using `make_builder` recursively.

                            // If we really need Struct support nested, using `arrow_json::reader` is best.
                            // But that's per-batch.
                            // Let's SKIP nested structs for now or just append NULL.
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
        } else {
            // Structured Mode: Flatten struct fields into top-level columns
            // We return arrays for each user field.
            // We do NOT wrap them in a struct "payload" column.
            // Spark usually expects flattened columns for the schema it requested.
            // Wait, if Spark asked for schema S, it expects columns matching S.
            // The connector usually appends metadata columns?
            // If user schema has "name", "age", do we return "message_id", ..., "name", "age"?
            // Yes, typically.
            // But we need to ensure the order matches what Spark expects.
            // Spark expects columns by name if possible, or position.
            // Let's append user arrays at the end.

            if let Some(builders) = self.struct_builders.as_mut() {
                for (i, builder) in builders.iter_mut().enumerate() {
                    let arr = builder.finish();
                    arrays.push(arr);
                    fields.push(self.struct_fields[i].clone());
                }
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
        let mut builder = ArrowBatchBuilder::new(None);

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
}
