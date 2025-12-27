//! # Native JNI Bridge for Spark Pub/Sub Connector
//!
//! This module provides the JNI (Java Native Interface) implementation for the Spark Pub/Sub connector.
//! It acts as the orchestration layer between Spark's JVM and the high-performance Rust data plane.
//!
//! Key roles:
//! 1. **FFI Orchestration**: Manages the Apache Arrow C Data Interface (FFI) for zero-copy data transfer.
//! 2. **Context Management**: Handles the lifecycle of Tokio runtimes and gRPC clients across JNI calls.
//! 3. **Type Translation**: Converts between JNI/Java types and Rust types using `robusta_jni`.

use robusta_jni::bridge;

mod logging;
mod pubsub;
mod arrow_convert;

use pubsub::PubSubClient;
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
    ///
    /// # Safety
    /// This function checks alignment but trusts that the pointers are valid objects if aligned.
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
/// 
/// This wrapper catches any panics that occur during the execution of the closure `f`,
/// logs the error details, and returns a default `error_val`.
/// This is critical for preventing Rust panics from crashing the JVM.
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
            
            // Try to get backtrace if available
            let backtrace = std::backtrace::Backtrace::capture();
            log::error!("Rust: Panic in JNI call: {}\nBacktrace: {:?}", msg, backtrace);
            
            // Also try eprintln if logging failed or panic was related to logging
            eprintln!("Rust: Panic in JNI call: {}\nBacktrace: {:?}", msg, backtrace);
            error_val
        }
    }
}


#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#[bridge]
mod jni {
    use crate::pubsub::PubSubClient;
    use robusta_jni::convert::{Signature, IntoJavaValue, FromJavaValue, TryIntoJavaValue, TryFromJavaValue};
    use robusta_jni::jni::JNIEnv;
    use robusta_jni::jni::objects::AutoLocal;
    use robusta_jni::jni::sys::jlong;
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::array::{StructArray, Array};
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    /// JNI wrapper for `NativeReader` on the Scala side.
    /// Manages message ingestion from Pub/Sub and conversion to Arrow batches.
    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue, FromJavaValue)]
    #[package(com.google.cloud.spark.pubsub)]
    pub struct NativeReader<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> NativeReader<'env, 'borrow> {
        
        /// Initializes a new `PubSubClient` with the given project and subscription.
        /// Returns a raw pointer (as `jlong`) to a `RustPartitionReader`.
        pub extern "jni" fn init(self, env: &JNIEnv, project_id: String, subscription_id: String, jitter_millis: i32, schema_json: String, partition_id: i32) -> jlong {
            // Centralized Logging Init: Ensure every new reader ensures logging is ready
            crate::logging::init(env);
            
            crate::safe_jni_call(0, || {
                crate::logging::set_context(&format!("[Partition: {}]", partition_id));
                log::info!("Rust: NativeReader.init called for project: {}, sub: {}", project_id, subscription_id);

                if jitter_millis > 0 {
                    let mut rng = rand::thread_rng();
                    let delay_ms = rand::Rng::gen_range(&mut rng, 0..(jitter_millis as u64));
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }

                // Parse configuration and schema
                let config = if !schema_json.is_empty() {
                    // Heuristic: If starts with '[', it's legacy array schema
                    if schema_json.trim_start().starts_with('[') {
                         let s = crate::arrow_convert::parse_simple_schema(&schema_json);
                         log::info!("Rust: Detected legacy schema parsing.");
                         crate::arrow_convert::ProcessingConfig {
                             arrow_schema: s,
                             format: crate::arrow_convert::DataFormat::Json,
                             avro_schema: None,
                             ca_certificate_path: None,
                         }
                    } else {
                        match crate::arrow_convert::parse_processing_config(&schema_json) {
                            Ok(c) => c,
                            Err(e) => {
                                log::warn!("Rust: Failed to parse processing config: {}", e);
                                crate::arrow_convert::ProcessingConfig {
                                    arrow_schema: None,
                                    format: crate::arrow_convert::DataFormat::Json,
                                    avro_schema: None,
                                    ca_certificate_path: None,
                                }
                            }
                        }
                    }
                } else {
                    crate::arrow_convert::ProcessingConfig {
                        arrow_schema: None,
                        format: crate::arrow_convert::DataFormat::Json,
                        avro_schema: None,
                        ca_certificate_path: None,
                    }
                };

                let rt = crate::pubsub::get_runtime();
                let _sub_key = (subscription_id.clone(), partition_id);

                log::info!("Rust: Creating new PubSubClient for partition {}", partition_id);
                let client_res = rt.block_on(async {
                    PubSubClient::new(&project_id, &subscription_id, config.ca_certificate_path.as_deref()).await
                });

                let client = match client_res {
                    Ok(c) => c,
                    Err(e) => {
                        let err_msg = format!("Rust: PubSubClient::new failed: {}", e);
                        log::error!("{}", err_msg);
                        let _ = env.throw_new("java/lang/RuntimeException", err_msg);
                        return 0;
                    }
                };

                let reader = Box::new(crate::RustPartitionReader {
                    rt,
                    client,
                    schema: config.arrow_schema,
                    format: config.format,
                    avro_schema: config.avro_schema,
                    partition_id,
                });
                Box::into_raw(reader) as jlong
            })
        }

        /// Fetches a batch of messages from the native buffer and exports them to Arrow memory addresses.
        /// Returns 1 if messages were fetched, 0 if empty, or negative on error.
        pub extern "jni" fn getNextBatch(self, _env: &JNIEnv, reader_ptr: jlong, batch_id: String, arrow_array_addr: jlong, arrow_schema_addr: jlong, max_messages: i32, wait_ms: jlong) -> i32 {
            crate::safe_jni_call(-100, || {
                if reader_ptr == 0 { return -1; }
                let reader = unsafe { &mut *(reader_ptr as *mut crate::RustPartitionReader) };
                crate::logging::set_context(&format!("[P: {}, B: {}]", reader.partition_id, batch_id));

                // 0. Check Reservoir Limit (Backpressure)
                // 0. Check Batch Map for excessive unacked if needed (Backpressure)
                // For now, relying on high-level library flow control.
                // But we can check HANDLE_MAP size.
                {
                    let unacked = crate::pubsub::ACK_HANDLE_MAP.len();
                    if unacked >= 200_000 {
                         log::warn!("Rust: ACK_HANDLE_MAP limit reached ({}). Applying backpressure.", unacked);
                         return 0; 
                    }
                }

                // Blocking fetch from native buffer (with short timeout)
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

                // Convert to Arrow
                let mut builder = crate::arrow_convert::ArrowBatchBuilder::new(
                    reader.schema.clone(),
                    reader.format,
                    reader.avro_schema.clone()
                );

                let mut ack_ids = Vec::with_capacity(messages.len());
                for msg in &messages {
                    builder.append(msg);
                    ack_ids.push(msg.ack_id.clone());
                }

                // Register batch in Ack Reservoir
                // Register batch in BATCH_ACK_MAP
                {
                    crate::pubsub::BATCH_ACK_MAP.insert(batch_id, ack_ids);
                }

                let (arrays, schema) = builder.finish();
                let struct_array = arrow::array::StructArray::from(arrow::array::ArrayData::from(
                    arrow::array::StructArray::try_from(arrow::record_batch::RecordBatch::try_new(schema, arrays).unwrap()).unwrap()
                ));

                // Export to FFI
                // Export to FFI
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

        /// Sends an asynchronous Acknowledgment request for the given list of message IDs.
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

        /// Flushes the native reservoir for the given committed batches.
        pub extern "jni" fn ackCommitted(self, _env: &JNIEnv, reader_ptr: jlong, batch_ids: Vec<String>) -> i32 {
            crate::safe_jni_call(-100, || {
                if reader_ptr == 0 { return -1; }
                let reader = unsafe { &*(reader_ptr as *const crate::RustPartitionReader) };

                let mut all_ack_ids = Vec::new();
                {
                    for id in batch_ids {
                        if let Some((_, ids)) = crate::pubsub::BATCH_ACK_MAP.remove(&id) {
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

        /// Returns the count of messages currently held in the off-heap Ack Reservoir.
        pub extern "jni" fn getUnackedCount(self, _env: &JNIEnv, _reader_ptr: jlong) -> i32 {
            crate::safe_jni_call(0, || {
                crate::pubsub::ACK_HANDLE_MAP.len() as i32
            })
        }

        /// Returns the estimated size (bytes) of messages buffered in the native layer (off-heap).
        pub extern "jni" fn getNativeMemoryUsageNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_buffered_bytes() as i64
            })
        }

        pub extern "jni" fn getIngestedBytesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_ingested_bytes() as i64
            })
        }

        pub extern "jni" fn getIngestedMessagesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_ingested_messages() as i64
            })
        }

        pub extern "jni" fn getReadErrorsNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_read_errors() as i64
            })
        }

        pub extern "jni" fn getRetryCountNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_retry_count() as i64
            })
        }

        pub extern "jni" fn getAckLatencyMicrosNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_ack_latency_micros() as i64
            })
        }

        /// Closes the reader, but returns the PubSubClient to the registry for reuse.
        pub extern "jni" fn close(self, _env: &JNIEnv, reader_ptr: jlong) {
            crate::safe_jni_call((), || {
                if reader_ptr != 0 {
                    let reader = unsafe { Box::from_raw(reader_ptr as *mut crate::RustPartitionReader) };
                    log::info!("Rust: Dropping PubSubClient and background task for partition {}", reader.partition_id);
                }
            })
        }
    }

    /// JNI wrapper for `NativeWriter` on the Scala side.
    /// Manages message publishing from Arrow batches to Pub/Sub.
    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue, FromJavaValue)]
    #[package(com.google.cloud.spark.pubsub)]
    pub struct NativeWriter<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> NativeWriter<'env, 'borrow> {
        /// Initializes a new `PublisherClient` with the given project and topic.
        /// Returns a raw pointer (as `jlong`) to a `RustPartitionWriter`.
        pub extern "jni" fn init(self, _env: &JNIEnv, project_id: String, topic_id: String, ca_certificate_path: String, partition_id: i32) -> jlong {
            crate::safe_jni_call(0, || {
                crate::logging::init(_env);
                crate::logging::set_context(&format!("[Sink P: {}]", partition_id));
                log::info!("Rust: NativeWriter.init called for project: {}, topic: {}", project_id, topic_id);

                let mut rng = rand::thread_rng();
                let delay_ms = rand::Rng::gen_range(&mut rng, 0..500);
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));

                let rt = crate::pubsub::get_runtime();
                
                let ca_path = if ca_certificate_path.is_empty() {
                    None
                } else {
                    Some(ca_certificate_path.as_str())
                };

                let client_res = rt.block_on(async {
                    crate::pubsub::PublisherClient::new(&project_id, &topic_id, ca_path).await
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

        /// Takes an Arrow batch from Spark via its C memory addresses, converts it to Pub/Sub messages,
        /// and publishes them to the configured topic.
        pub extern "jni" fn writeBatch(self, _env: &JNIEnv, writer_ptr: jlong, arrow_array_addr: jlong, arrow_schema_addr: jlong) -> i32 {
            crate::safe_jni_call(-100, || {
                if writer_ptr == 0 {
                    return -1;
                }
                // SAFETY: Java side provides valid pointers to FFI structures
                let writer = unsafe { &mut *(writer_ptr as *mut crate::RustPartitionWriter) };
                crate::logging::set_context(&format!("[Sink P: {}]", writer.partition_id));
                
                // SAFETY: We own the memory addresses passed from Java via the C Data Interface.
                // We use std::ptr::read to take ownership of the FFI structs.
                // SAFETY: We use FFIGuard to validate pointers before importing
                unsafe {
                    let guard = match crate::FFIGuard::new(arrow_array_addr, arrow_schema_addr) {
                        Ok(g) => g,
                        Err(e) => {
                            log::error!("Rust: FFI Guard failed during write import: {}", e);
                            return -2;
                        }
                    };

                    // Import from FFI using official API
                    // Note: `from_ffi` validates the schema/array internally
                    let array_data = match arrow::ffi::from_ffi(std::ptr::read(guard.array), &std::ptr::read(guard.schema)) {
                        Ok(data) => data,
                        Err(e) => {
                            log::error!("Rust: FFI Import failed: {:?}", e);
                            return -3;
                        }
                    };
                    
                    let array = StructArray::from(array_data);
                    
                    // Convert to PubsubMessages
                    let reader = crate::arrow_convert::ArrowBatchReader::new(&array);
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
                } // end unsafe
            })
        }



        pub extern "jni" fn getPublishedBytesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_published_bytes() as i64
            })
        }

        pub extern "jni" fn getPublishedMessagesNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_published_messages() as i64
            })
        }

        pub extern "jni" fn getWriteErrorsNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_write_errors() as i64
            })
        }

        pub extern "jni" fn getRetryCountNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_retry_count() as i64
            })
        }

        pub extern "jni" fn getPublishLatencyMicrosNative(self, _env: &JNIEnv) -> i64 {
            crate::safe_jni_call(0, || {
                crate::pubsub::get_publish_latency_micros() as i64
            })
        }

        pub extern "jni" fn close(self, _env: &JNIEnv, writer_ptr: jlong, timeout_ms: jlong) -> i32 {
            crate::safe_jni_call(-99, || {
                if writer_ptr != 0 {
                    let writer = unsafe { Box::from_raw(writer_ptr as *mut crate::RustPartitionWriter) };
                    // Flush before dropping (which kills the runtime)
                    let flush_res = writer.rt.block_on(async {
                        // Use provided timeout or default to 30s if something is wrong (though Scala should enforce valid long)
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
                    // writer is dropped here, closing connections
                    return flush_res;
                }
                0
            })
        }
    }
}



/// Internal state for a Spark partition reader, including its dedicated Tokio runtime.
/// 
/// This struct holds the resources needed to pull messages from Pub/Sub and convert them
/// to Arrow batches. It is maintained in Rust memory and referenced by Spark via a raw pointer.
pub struct RustPartitionReader {
    rt: &'static Runtime,
    client: PubSubClient,
    schema: Option<arrow::datatypes::SchemaRef>,
    format: crate::arrow_convert::DataFormat,
    avro_schema: Option<apache_avro::Schema>,
    partition_id: i32,
}

pub struct RustPartitionWriter {
    rt: &'static Runtime,
    client: crate::pubsub::PublisherClient,
    partition_id: i32,
}
