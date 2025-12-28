//! # Native JNI Bridge for Spark Pub/Sub Connector
//!
//! This module provides the JNI (Java Native Interface) implementation for the Spark Pub/Sub connector.
//! It acts as the orchestration layer between Spark's JVM and the high-performance Rust data plane.

use robusta_jni::bridge;

mod core;
mod source;
mod sink;
mod schema;
mod diagnostics;

use tokio::runtime::Runtime;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use robusta_jni::jni::sys::jlong;

/// Helper to safeguard FFI pointers before accessing them.
struct FFIGuard {
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
}

impl FFIGuard {
    /// Validates that FFI pointers are non-null and properly aligned.
    unsafe fn new(array: jlong, schema: jlong) -> Result<Self, &'static str> {
        if array == 0 || schema == 0 {
            return Err("Received NULL pointer for Arrow FFI");
        }
        
        let array_ptr = array as *mut FFI_ArrowArray;
        let schema_ptr = schema as *mut FFI_ArrowSchema;

        if array_ptr.align_offset(std::mem::align_of::<FFI_ArrowArray>()) != 0 {
            return Err("Arrow Array pointer is misaligned");
        }
        if schema_ptr.align_offset(std::mem::align_of::<FFI_ArrowSchema>()) != 0 {
            return Err("Arrow Schema pointer is misaligned");
        }

        Ok(Self {
            array: array_ptr,
            schema: schema_ptr,
        })
    }
}

/// Helper for panic safety in JNI calls.
fn safe_jni_call<R, F>(error_val: R, f: F) -> R
where
    F: FnOnce() -> R + std::panic::UnwindSafe,
{
    match std::panic::catch_unwind(f) {
        Ok(res) => res,
        Err(e) => {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                format!("Panic: {}", s)
            } else if let Some(s) = e.downcast_ref::<String>() {
                format!("Panic: {}", s)
            } else {
                "Panic occurred (unknown cause)".to_string()
            };
            
            let backtrace = std::backtrace::Backtrace::capture();
            log::error!("Rust: Panic in JNI call: {}\nBacktrace: {:?}", msg, backtrace);
            eprintln!("Rust: Panic in JNI call: {}\nBacktrace: {:?}", msg, backtrace);
            error_val
        }
    }
}


#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#[bridge]
mod source_jni {
    use robusta_jni::convert::{Signature, IntoJavaValue, FromJavaValue, TryIntoJavaValue, TryFromJavaValue};
    use robusta_jni::jni::JNIEnv;
    use robusta_jni::jni::objects::AutoLocal;
    use robusta_jni::jni::sys::jlong;
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::array::{StructArray, Array};
    
    /// JNI wrapper for `NativeReader` on the Scala side.
    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue, FromJavaValue)]
    #[package(com.google.cloud.spark.pubsub.source)]
    pub struct NativeReader<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> NativeReader<'env, 'borrow> {
        
        pub extern "jni" fn init(self, env: &JNIEnv, project_id: String, subscription_id: String, jitter_millis: i32, schema_json: String, partition_id: i32) -> jlong {
            crate::diagnostics::logging::init(env);
            
            crate::safe_jni_call(0, || {
                crate::diagnostics::logging::set_context(&format!("[Partition: {}]", partition_id));
                log::info!("Rust: NativeReader.init called for partition: {}", partition_id);

                if jitter_millis > 0 {
                    let mut rng = rand::thread_rng();
                    let delay_ms = rand::Rng::gen_range(&mut rng, 0..(jitter_millis as u64));
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }

                let (client, config) = if let Some(c) = crate::core::client::CLIENT_REGISTRY.get(&partition_id) {
                    log::info!("Rust: Reusing existing PubSubClient for partition {}", partition_id);
                    
                    let cfg = if !schema_json.is_empty() {
                         if schema_json.trim_start().starts_with('[') {
                             let s = crate::schema::parse_simple_schema(&schema_json);
                             crate::schema::ProcessingConfig {
                                 arrow_schema: s,
                                 format: crate::schema::DataFormat::Json,
                                 avro_schema: None,
                                 ca_certificate_path: None,
                             }
                         } else {
                             crate::schema::parse_processing_config(&schema_json).unwrap_or_default()
                         }
                    } else {
                        crate::schema::ProcessingConfig::default()
                    };
                    
                    (c.value().clone(), cfg)
                } else {
                    log::info!("Rust: Creating new PubSubClient for partition {}", partition_id);
                    let cfg = if !schema_json.is_empty() {
                        if schema_json.trim_start().starts_with('[') {
                             let s = crate::schema::parse_simple_schema(&schema_json);
                             crate::schema::ProcessingConfig {
                                 arrow_schema: s,
                                 format: crate::schema::DataFormat::Json,
                                 avro_schema: None,
                                 ca_certificate_path: None,
                             }
                        } else {
                            match crate::schema::parse_processing_config(&schema_json) {
                                Ok(c) => c,
                                Err(e) => {
                                    log::warn!("Rust: Failed to parse processing config: {}", e);
                                    crate::schema::ProcessingConfig::default()
                                }
                            }
                        }
                    } else {
                        crate::schema::ProcessingConfig::default()
                    };

                    let rt = crate::core::runtime::get_runtime();
                    let client_res = rt.block_on(async {
                        crate::core::client::PubSubClient::new(&project_id, &subscription_id, cfg.ca_certificate_path.as_deref()).await
                    });

                    match client_res {
                        Ok(c) => {
                            let shared_client = std::sync::Arc::new(c);
                            crate::core::client::CLIENT_REGISTRY.insert(partition_id, shared_client.clone());
                            (shared_client, cfg)
                        },
                        Err(e) => {
                            let err_msg = format!("Rust: PubSubClient::new failed: {}", e);
                            log::error!("{}", err_msg);
                            let _ = env.throw_new("java/lang/RuntimeException", err_msg);
                            return 0;
                        }
                    }
                };

                let reader = Box::new(crate::RustPartitionReader {
                    rt: crate::core::runtime::get_runtime(),
                    client,
                    schema: config.arrow_schema,
                    format: config.format,
                    avro_schema: config.avro_schema,
                    partition_id,
                });
                Box::into_raw(reader) as jlong
            })
        }

        pub extern "jni" fn getNextBatch(self, _env: &JNIEnv, reader_ptr: jlong, batch_id: String, arrow_array_addr: jlong, arrow_schema_addr: jlong, max_messages: i32, wait_ms: jlong) -> i32 {
            crate::safe_jni_call(-100, || {
                if reader_ptr == 0 { return -1; }
                let reader = unsafe { &mut *(reader_ptr as *mut crate::RustPartitionReader) };
                crate::diagnostics::logging::set_context(&format!("[P: {}, B: {}]", reader.partition_id, batch_id));

                {
                    let unacked = crate::source::ACK_HANDLE_MAP.len();
                    // Increased limit to 1,000,000 to accommodate 12-24 partitions (which can pull 20k each)
                    // without hitting a deadlock before the first commit.
                    if unacked >= 1_000_000 {
                         log::warn!("Rust: ACK_HANDLE_MAP limit reached ({}). Applying backpressure.", unacked);
                         return 0; 
                    }
                }

                let messages = match reader.rt.block_on(async { reader.client.fetch_batch(max_messages as usize, wait_ms as u64).await }) {
                    Ok(msgs) => msgs,
                    Err(e) => {
                        log::error!("Rust: fetch_batch failed: {:?}", e);
                        return -2;
                    }
                };

                if messages.is_empty() {
                    return 0;
                }

                let mut builder = crate::schema::builder::ArrowBatchBuilder::new(
                    reader.schema.clone(),
                    reader.format,
                    reader.avro_schema.clone()
                );

                let mut ack_ids = Vec::with_capacity(messages.len());
                let mut total_bytes = 0usize;
                let byte_threshold = 50 * 1024 * 1024;

                for msg in &messages {
                    if let Some(m) = &msg.message {
                        total_bytes += m.data.len();
                    }
                    builder.append(msg);
                    ack_ids.push(msg.ack_id.clone());
                    
                    if total_bytes >= byte_threshold {
                        log::info!("Rust: Batch byte threshold reached ({} bytes). Stopping ingestion for this batch.", total_bytes);
                        break;
                    }
                }

                {
                    let key = format!("p{}-{}", reader.partition_id, batch_id);
                    crate::source::BATCH_ACK_MAP.insert(key, ack_ids);
                }

                let (arrays, schema) = builder.finish();
                let struct_array = arrow::array::StructArray::from(arrow::array::ArrayData::from(
                    arrow::array::StructArray::try_from(arrow::record_batch::RecordBatch::try_new(schema, arrays).unwrap()).unwrap()
                ));

                unsafe {
                    match crate::FFIGuard::new(arrow_array_addr, arrow_schema_addr) {
                        Ok(guard) => {
                            let data = struct_array.to_data();
                            let ffi_array = FFI_ArrowArray::new(&data);
                            let ffi_schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();

                            std::ptr::write(guard.array, ffi_array);
                            std::ptr::write(guard.schema, ffi_schema);
                        },
                        Err(e) => {
                             log::error!("Rust: FFI Guard failed during read export: {}", e);
                             return -3;
                        }
                    }
                }

                1
            })
        }

        pub extern "jni" fn acknowledge(self, _env: &JNIEnv, reader_ptr: jlong, ack_ids: Vec<String>) -> i32 {
            crate::safe_jni_call(-100, || {
                if reader_ptr == 0 { return -1; }
                let reader = unsafe { &*(reader_ptr as *const crate::RustPartitionReader) };

                match reader.rt.block_on(async { reader.client.acknowledge(ack_ids).await }) {
                    Ok(_) => 1,
                    Err(e) => {
                        log::error!("Rust: acknowledge failed: {:?}", e);
                        -2
                    }
                }
            })
        }

        pub extern "jni" fn ackCommitted(self, _env: &JNIEnv, reader_ptr: jlong, batch_ids: Vec<String>) -> i32 {
            crate::safe_jni_call(-100, || {
                if reader_ptr == 0 { return -1; }
                let reader = unsafe { &*(reader_ptr as *const crate::RustPartitionReader) };

                let mut all_ack_ids = Vec::new();
                {
                    for id in batch_ids {
                        let key = format!("p{}-{}", reader.partition_id, id);
                        if let Some((_, ids)) = crate::source::BATCH_ACK_MAP.remove(&key) {
                            all_ack_ids.extend(ids);
                        }
                    }
                }

                if all_ack_ids.is_empty() { return 0; }

                match reader.rt.block_on(async { reader.client.acknowledge(all_ack_ids).await }) {
                    Ok(_) => 1,
                    Err(e) => {
                        log::error!("Rust: ackCommitted failed: {:?}", e);
                        -2
                    }
                }
            })
        }

        pub extern "jni" fn getUnackedCount(self, _env: &JNIEnv, _reader_ptr: jlong) -> i32 {
            crate::safe_jni_call(0, || {
                crate::source::ACK_HANDLE_MAP.len() as i32
            })
        }

        pub extern "jni" fn getNativeMemoryUsageNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_buffered_bytes() as i64
            })
        }

        pub extern "jni" fn getIngestedBytesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_ingested_bytes() as i64
            })
        }

        pub extern "jni" fn getIngestedMessagesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_ingested_messages() as i64
            })
        }

        pub extern "jni" fn getReadErrorsNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_read_errors() as i64
            })
        }

        pub extern "jni" fn getRetryCountNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_retry_count() as i64
            })
        }

        pub extern "jni" fn getAckLatencyMicrosNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_ack_latency_micros() as i64
            })
        }

        pub extern "jni" fn close(self, _env: &JNIEnv, reader_ptr: jlong) {
            crate::safe_jni_call((), || {
                if reader_ptr != 0 {
                    let _reader = unsafe { Box::from_raw(reader_ptr as *mut crate::RustPartitionReader) };
                }
            })
        }
    }
}

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#[bridge]
mod sink_jni {
    use robusta_jni::convert::{Signature, IntoJavaValue, FromJavaValue, TryIntoJavaValue, TryFromJavaValue};
    use robusta_jni::jni::JNIEnv;
    use robusta_jni::jni::objects::AutoLocal;
    use robusta_jni::jni::sys::jlong;
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::array::{StructArray, Array};

    /// JNI wrapper for `NativeWriter` on the Scala side.
    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue, FromJavaValue)]
    #[package(com.google.cloud.spark.pubsub.sink)]
    pub struct NativeWriter<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> NativeWriter<'env, 'borrow> {
        pub extern "jni" fn init(self, _env: &JNIEnv, project_id: String, topic_id: String, ca_certificate_path: String, partition_id: i32) -> jlong {
            crate::safe_jni_call(0, || {
                crate::diagnostics::logging::init(_env);
                crate::diagnostics::logging::set_context(&format!("[Sink P: {}]", partition_id));
                log::info!("Rust: NativeWriter.init called for project: {}, topic: {}", project_id, topic_id);

                let rt = crate::core::runtime::get_runtime();
                let ca_path = if ca_certificate_path.is_empty() { None } else { Some(ca_certificate_path.as_str()) };

                let client_res = rt.block_on(async {
                    crate::sink::PublisherClient::new(&project_id, &topic_id, ca_path).await
                });

                match client_res {
                    Ok(c) => {
                        let writer = Box::new(crate::RustPartitionWriter {
                            rt,
                            client: c,
                            partition_id,
                        });
                        Box::into_raw(writer) as jlong
                    },
                    Err(e) => {
                        log::error!("Rust: Failed to define publisher: {:?}", e);
                        0
                    }
                }
            })
        }

        pub extern "jni" fn writeBatch(self, _env: &JNIEnv, writer_ptr: jlong, arrow_array_addr: jlong, arrow_schema_addr: jlong) -> i32 {
            crate::safe_jni_call(-100, || {
                if writer_ptr == 0 { return -1; }
                let writer = unsafe { &mut *(writer_ptr as *mut crate::RustPartitionWriter) };
                crate::diagnostics::logging::set_context(&format!("[Sink P: {}]", writer.partition_id));
                
                unsafe {
                    let guard = match crate::FFIGuard::new(arrow_array_addr, arrow_schema_addr) {
                        Ok(g) => g,
                        Err(e) => {
                            log::error!("Rust: FFI Guard failed during write import: {}", e);
                            return -2;
                        }
                    };

                    let array_data = match arrow::ffi::from_ffi(std::ptr::read(guard.array), &std::ptr::read(guard.schema)) {
                        Ok(data) => data,
                        Err(e) => {
                            log::error!("Rust: FFI Import failed: {:?}", e);
                            return -3;
                        }
                    };
                    
                    let array = StructArray::from(array_data);
                    let reader = crate::schema::reader::ArrowBatchReader::new(&array);
                    let msgs = match reader.to_pubsub_messages() {
                         Ok(m) => m,
                         Err(e) => {
                             log::error!("Rust: Failed to convert batch to PubsubMessages: {:?}", e);
                             return -4;
                         }
                    };

                    let res = writer.rt.block_on(async {
                         writer.client.publish_batch(msgs).await
                    });
                    
                    if let Err(e) = res {
                        log::error!("Rust: Failed to publish batch: {:?}", e);
                        return -5;
                    }
                    1
                }
            })
        }

        pub extern "jni" fn getPublishedBytesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_published_bytes() as i64
            })
        }

        pub extern "jni" fn getPublishedMessagesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_published_messages() as i64
            })
        }

        pub extern "jni" fn getWriteErrorsNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_write_errors() as i64
            })
        }

        pub extern "jni" fn getRetryCountNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_retry_count() as i64
            })
        }

        pub extern "jni" fn getPublishLatencyMicrosNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::core::metrics::get_publish_latency_micros() as i64
            })
        }

        pub extern "jni" fn close(self, _env: &JNIEnv, writer_ptr: jlong, timeout_ms: jlong) -> i32 {
            crate::safe_jni_call(-99, || {
                if writer_ptr != 0 {
                    let writer = unsafe { Box::from_raw(writer_ptr as *mut crate::RustPartitionWriter) };
                    let flush_res = writer.rt.block_on(async {
                        let timeout = if timeout_ms > 0 {
                            std::time::Duration::from_millis(timeout_ms as u64)
                        } else {
                            std::time::Duration::from_secs(30)
                        };

                        if let Err(e) = writer.client.flush(timeout).await {
                            log::error!("Rust: Writer close flush failed: {}", e);
                            return -1;
                        }
                        0
                    });
                    return flush_res;
                }
                0
            })
        }
    }
}

pub struct RustPartitionReader {
    rt: &'static Runtime,
    client: std::sync::Arc<crate::core::client::PubSubClient>,
    schema: Option<arrow::datatypes::SchemaRef>,
    format: crate::schema::DataFormat,
    avro_schema: Option<apache_avro::Schema>,
    partition_id: i32,
}

pub struct RustPartitionWriter {
    rt: &'static Runtime,
    client: crate::sink::PublisherClient,
    partition_id: i32,
}
