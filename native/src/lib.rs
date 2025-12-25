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

/// ABI-compatible mirrors for Arrow FFI structures. 
/// These are used to validate memory layouts and pointers before importing them into `arrow-rs`.
/// They are necessary because the official `FFI_ArrowSchema` and `FFI_ArrowArray` do not expose 
/// all internal fields needed for proactive validation.
#[repr(C)]
pub struct FfiArrowSchemaMirror {
    pub format: *const std::ffi::c_char,
    pub name: *const std::ffi::c_char,
    pub metadata: *const std::ffi::c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut FFI_ArrowSchema,
    pub dictionary: *mut FFI_ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut FFI_ArrowSchema)>,
    pub private_data: *mut std::ffi::c_void,
}

#[repr(C)]
pub struct FfiArrowArrayMirror {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const std::ffi::c_void,
    pub children: *mut *mut FFI_ArrowArray,
    pub dictionary: *mut FFI_ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>,
    pub private_data: *mut std::ffi::c_void,
}

/// # Safety
/// This is a no-op release function for Arrow FFI. It does not free any memory.
pub unsafe extern "C" fn noop_release_array(_array: *mut FFI_ArrowArray) {}

/// # Safety
/// This is a no-op release function for Arrow FFI. It does not free any memory.
pub unsafe extern "C" fn noop_release_schema(_schema: *mut FFI_ArrowSchema) {}

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
    use crate::arrow_convert::ArrowBatchBuilder;
    use robusta_jni::convert::{Signature, IntoJavaValue, FromJavaValue, TryIntoJavaValue, TryFromJavaValue};
    use robusta_jni::jni::JNIEnv;
    use robusta_jni::jni::objects::AutoLocal;
    use robusta_jni::jni::sys::jlong;
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::array::{Array, StructArray};
    
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
        pub extern "jni" fn init(self, env: &JNIEnv, project_id: String, subscription_id: String, jitter_millis: i32, schema_json: String) -> jlong {
            crate::safe_jni_call(0, || {
                // Initialize JNI logging (safe to call multiple times)
                crate::logging::init(env);
                println!("Rust Raw: NativeReader.init called for project: {}", project_id);
                log::info!("Rust: NativeReader.init called for project: {}", project_id);

                if jitter_millis > 0 {
                    let mut rng = rand::thread_rng();
                    let delay_ms = rand::Rng::gen_range(&mut rng, 0..(jitter_millis as u64));
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }

                // Parse configuration and schema
                let (arrow_schema, format, avro_schema) = if !schema_json.is_empty() {
                    // Heuristic: If starts with '[', it's legacy array schema
                    if schema_json.trim_start().starts_with('[') {
                         let s = crate::arrow_convert::parse_simple_schema(&schema_json);
                         log::info!("Rust: Detected legacy schema parsing.");
                         (s, crate::arrow_convert::DataFormat::Json, None)
                    } else {
                        match crate::arrow_convert::parse_processing_config(&schema_json) {
                            Ok(c) => (c.arrow_schema, c.format, c.avro_schema),
                            Err(e) => {
                                log::warn!("Rust: Failed to parse processing config: {}", e);
                                (None, crate::arrow_convert::DataFormat::Json, None)
                            }
                        }
                    }
                } else {
                    (None, crate::arrow_convert::DataFormat::Json, None)
                };

                // Use Global Runtime
                let rt = crate::pubsub::get_runtime();
                
                // Create Publisher Client
                let client_res = rt.block_on(async {
                    PubSubClient::new(&project_id, &subscription_id).await
                });

                match client_res {
                    Ok(client) => {
                        crate::pubsub::start_deadline_manager(rt);

                        let reader = Box::new(crate::RustPartitionReader {
                            rt,
                            client,
                            schema: arrow_schema,
                            format,
                            avro_schema,
                        });
                        log::info!("Rust: NativeReader.init success. Pointer: {:?}", reader.as_ref() as *const _);
                        Box::into_raw(reader) as jlong
                    },
                    Err(e) => {
                        log::error!("Rust: Failed to create Pub/Sub client: {:?}", e);
                        println!("Rust Raw: Failed to create Pub/Sub client for project {}, sub {}: {:?}", project_id, subscription_id, e);
                        eprintln!("Rust: Failed to create Pub/Sub client for project {}, sub {}: {:?}", project_id, subscription_id, e);
                        0
                    }
                }
            })
        }

        /// Fetches the next batch of messages from the background queue, converts them to an Arrow batch,
        /// and exports them directly into the memory addresses provided by Spark (out_array, out_schema).
        /// Returns 1 if a batch was produced, 0 if no messages are available, or a negative error code.
        pub extern "jni" fn getNextBatch(self, _env: &JNIEnv, reader_ptr: jlong, batch_id: String, out_array: jlong, out_schema: jlong) -> i32 {
            crate::safe_jni_call(-100, || {
                if reader_ptr == 0 {
                    return -1;
                }
                // SAFETY: Java side provides valid pointers to FFI structures
                let reader = unsafe { &mut *(reader_ptr as *mut crate::RustPartitionReader) };
                
                log::info!("Rust: getNextBatch called for batch: {}", &batch_id);
                let msgs = match reader.rt.block_on(async {
                    reader.client.fetch_batch(1000).await
                }) {
                    Ok(m) => m,
                    Err(e) => {
                        log::error!("Rust: fetch_batch failed: {}", e);
                        return -5; // Error code for permanent failure / channel closed
                    }
                };
                
                if msgs.is_empty() {
                    return 0;
                }

                // Store Ack IDs in the native reservoir before exporting to Arrow
                let ack_ids: Vec<String> = msgs.iter().map(|m| m.ack_id.clone()).collect();
                {
                    let mut reservoir = crate::pubsub::ACK_RESERVOIR.lock().unwrap_or_else(|e| e.into_inner());
                    let entry = reservoir.entry(batch_id.clone())
                        .or_insert_with(|| (tokio::time::Instant::now(), std::collections::HashMap::new()));
                    entry.1.entry(reader.client.subscription_name.clone())
                        .or_insert_with(Vec::new)
                        .extend(ack_ids);
                }

                let mut builder = ArrowBatchBuilder::new(reader.schema.clone(), reader.format, reader.avro_schema.clone());
                for msg in msgs {
                    builder.append(&msg);
                }
                
                let (arrays, schema) = builder.finish();
                
                let batch = match arrow::record_batch::RecordBatch::try_new(schema, arrays) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("Rust: Failed to create RecordBatch: {:?}", e);
                        return -2;
                    }
                };
                
                // Convert to FFI structs via StructArray
                let struct_array = StructArray::from(batch);
                let (array_val, schema_val) = match arrow::ffi::to_ffi(&struct_array.into_data()) {
                    Ok(ffi) => ffi,
                    Err(e) => {
                        log::error!("Rust: Failed to export to FFI: {:?}", e);
                        return -3;
                    }
                };

                // Write to C pointers provided by Java
                let out_array_ptr = out_array as *mut FFI_ArrowArray;
                let out_schema_ptr = out_schema as *mut FFI_ArrowSchema;

                // SAFETY: We write to the provided pointers using the C Data Interface.
                unsafe {
                    std::ptr::write(out_array_ptr, array_val);
                    std::ptr::write(out_schema_ptr, schema_val);
                }
                
                log::info!("Rust: getNextBatch returning success (1) for batch: {}", &batch_id);
                1
            })
        }

        // Removed storeAcksFromArrow because ack storage is now integrated into getNextBatch

        /// Sends an asynchronous Acknowledgment request for a list of message IDs.
        /// This is used for "At-Least-Once" delivery guarantees in Spark.
        pub extern "jni" fn acknowledge(self, _env: &JNIEnv, reader_ptr: jlong, ack_ids: Vec<String>) -> i32 {
            crate::safe_jni_call(0, || {
                if reader_ptr == 0 {
                    return -1;
                }
                let reader = unsafe { &mut *(reader_ptr as *mut crate::RustPartitionReader) };
                
                let res = reader.rt.block_on(async {
                    reader.client.acknowledge(ack_ids).await
                });
                
                match res {
                    Ok(_) => 1,
                    Err(e) => {
                        log::error!("Rust: Failed to acknowledge batch: {:?}", e);
                        0
                    }
                }
            })
        }

        /// Flushes the native reservoir for the given committed batches.
        pub extern "jni" fn ackCommitted(self, _env: &JNIEnv, reader_ptr: jlong, batch_ids: Vec<String>) -> i32 {
            crate::safe_jni_call(-100, || {
                if reader_ptr == 0 || batch_ids.is_empty() { return 1; }
                let reader = unsafe { &mut *(reader_ptr as *mut crate::RustPartitionReader) };
                
                let mut to_ack = Vec::new();
                {
                    let mut reservoir = crate::pubsub::ACK_RESERVOIR.lock().unwrap_or_else(|e| e.into_inner());
                    for id in batch_ids {
                        if let Some((_, subs)) = reservoir.get_mut(&id) {
                            if let Some(ids) = subs.remove(&reader.client.subscription_name) {
                                to_ack.extend(ids);
                            }
                        }
                    }
                    // Cleanup empty batches
                    reservoir.retain(|_, (_, subs)| !subs.is_empty());
                }

                if to_ack.is_empty() { return 1; }

                let res = reader.rt.block_on(async {
                    reader.client.acknowledge(to_ack).await
                });
                
                if res.is_ok() { 1 } else { 0 }
            })
        }

        pub extern "jni" fn getUnackedCount(self, _env: &JNIEnv, _reader_ptr: jlong) -> i32 {
            crate::safe_jni_call(-1, || {
                let reservoir = crate::pubsub::ACK_RESERVOIR.lock().unwrap_or_else(|e| e.into_inner());
                let mut count = 0;
                for (_batch, (_, subs)) in reservoir.iter() {
                    for (_sub, ids) in subs {
                        count += ids.len();
                    }
                }
                count as i32
            })
        }

        /// Destroys the `RustPartitionReader` and shuts down its Tokio runtime.
        pub extern "jni" fn close(self, _env: &JNIEnv, reader_ptr: jlong) {
            crate::safe_jni_call((), || {
                if reader_ptr != 0 {
                    let _reader = unsafe { Box::from_raw(reader_ptr as *mut crate::RustPartitionReader) };
                    // Runtime will be dropped here, shutting down connections
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
        pub extern "jni" fn init(self, _env: &JNIEnv, project_id: String, topic_id: String) -> jlong {
            crate::safe_jni_call(0, || {
                // Guideline 4: Staggered Initialization
                // Initialize logging (safe to call multiple times)
                crate::logging::init(_env);
                log::info!("Rust: NativeWriter.init called for project: {}", project_id);

                let mut rng = rand::thread_rng();
                let delay_ms = rand::Rng::gen_range(&mut rng, 0..500);
                std::thread::sleep(std::time::Duration::from_millis(delay_ms));

                let rt = crate::pubsub::get_runtime();
                
                let client_res = rt.block_on(async {
                    crate::pubsub::PublisherClient::new(&project_id, &topic_id).await
                });

                match client_res {
                    Ok(c) => {
                        let writer = Box::new(crate::RustPartitionWriter {
                            rt,
                            client: c,
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
                
                // SAFETY: We own the memory addresses passed from Java via the C Data Interface.
                // We use std::ptr::read to take ownership of the FFI structs.
                unsafe {
                    let array_ptr = arrow_array_addr as *mut FFI_ArrowArray;
                    let schema_ptr = arrow_schema_addr as *mut FFI_ArrowSchema;

                    // 1. Validate Schema before taking full ownership (peek without dropping)
                    // We cast to our mirror struct to check fields.
                    // Note: We don't read it yet, just inspect the raw pointer content.
                    let schema_mirror = &*(schema_ptr as *const crate::FfiArrowSchemaMirror);
                    
                    if schema_mirror.n_children > 0 {
                        if schema_mirror.children.is_null() {
                            log::error!("Rust: FFI Error - Schema n_children is {} but children pointer is NULL", schema_mirror.n_children);
                            // If we return here, we haven't called std::ptr::read(), so we haven't taken ownership "in Rust terms".
                            // However, the caller (Java/Spark) expects us to take ownership.
                            // If we don't, Spark might double free or leak?
                            // Spark Arrow: "The consumer is responsible for releasing the memory."
                            // If we error, we must still release the input if we "consumed" the responsibility.
                            // But usually if FFI fails, we should release what we got.
                            // To be safe: we import it to release it.
                            let schema_val = std::ptr::read(schema_ptr);
                             // Clean up properly by letting Arrow drop it?
                             // FFI_ArrowSchema implements Drop? No, we need to call release callback manually if we don't convert.
                             // Best way: Import fully, then drop.
                             let _ = arrow::ffi::from_ffi(std::mem::zeroed(), &schema_val); // This is hacky.
                             // Better: Just release manually using the callback.
                             if let Some(release) = schema_mirror.release {
                                 release(schema_ptr);
                             }
                            return -20;
                        }
                    }

                    // Now we take ownership safely
                    let array_val_raw = std::ptr::read(array_ptr);
                    let schema_val_raw = std::ptr::read(schema_ptr);
                    
                    // Arrays are now owned. If we fail from here on, we MUST drop them.
                    // arrow::ffi::from_ffi takes these by value (moves them).
                    // But wait, from_ffi call signature: `pub fn from_ffi(array: FFI_ArrowArray, schema: &FFI_ArrowSchema) -> Result<ArrayData, ArrowError>`
                    // It takes `array` by value (consumes), but `schema` by reference?? 
                    // No, `from_ffi` signature is: `fn from_ffi(array: FFI_ArrowArray, schema: &FFI_ArrowSchema)`.
                    // It does NOT consume schema. We still own schema.

                    // 3. Import into arrow-rs
                    let array_data = match arrow::ffi::from_ffi(array_val_raw, &schema_val_raw) {
                        Ok(data) => data,
                        Err(e) => {
                            log::error!("Rust: Failed to import Arrow array from FFI: {:?}", e);
                            // We own array_val_raw and schema_val_raw. We must drop/release them.
                            // Schema we still own. Array was moved into from_ffi? No, only matching success? 
                            // `from_ffi` takes ownership of `array`. If it fails, does it drop `array`?
                            // Looking at arrow-rs source: if it returns Err, it likely drops the input FFI_ArrowArray.
                            // But we DEFINITELY own schema_val_raw. Arrow-rs struct doesn't implement Drop that calls release.
                            // We need to validly release the schema.
                             // Actually, FFI_ArrowSchema and FFI_ArrowArray DO NOT implement Drop to call release.
                             // We must call release manually if we don't successfully convert to something that manages it.
                            
                            // NOTE: Arrow C Data Interface says: "The release callback is responsible for releasing the memory."
                            // If we successfully imported `array_data` (Data type), it manages release.
                            // If `from_ffi` failed, we need to ensure release is called.
                            // THIS IS TRICKY. 
                            // Safe bet: We check if they are released.
                            // Actually, let's just use the `ArrowArray` and `ArrowSchema` wrappers from arrow::ffi if possible?
                            // They are not exposed easily.
                            
                            // Manual release fall-back:
                            // We just cast back to mirror and call release.
                            let sm: crate::FfiArrowSchemaMirror = std::mem::transmute(schema_val_raw);
                            if let Some(r) = sm.release { r(schema_ptr); } // Use original pointer or struct?
                            // We moved std::ptr::read, so the struct is on stack. We call release on the STRUCT pointer?
                            // The release callback expects `*mut FFI_ArrowSchema`.
                            // We need to pass a pointer to our stack object? No, the release callback usually frees the `private_data`.
                            // It doesn't free the struct itself if it's stack allocated, but it frees the buffers.
                            // But `std::ptr::read` COPIED the struct content to stack. The original pointer `schema_ptr` points to valid memory?
                            // Yes, in C Data Interface, the struct itself is allocated by caller.
                            // We should call release on the `schema_ptr`!
                            // wait, we `std::ptr::read` it, so we effectively "moved" it. 
                            // But the resources are pointed to by fields.
                            // If we call release on `schema_ptr`, it cleans up resources.
                            // Our stack copy is just a copy of pointers.
                            // CORRECT FIX: Just call release on the ORIGINAL pointers if import fails.
                             let sm_ref = &*(schema_ptr as *const crate::FfiArrowSchemaMirror);
                             if let Some(r) = sm_ref.release { r(schema_ptr); }

                             let am_ref = &*(array_ptr as *const crate::FfiArrowArrayMirror);
                             if let Some(r) = am_ref.release { r(array_ptr); }

                            return -2;
                        }
                    };
                    
                    let array = StructArray::from(array_data);
                    
                    // Convert to PubsubMessages
                    let reader = crate::arrow_convert::ArrowBatchReader::new(&array);
                    let msgs = match reader.to_pubsub_messages() {
                         Ok(m) => m,
                         Err(e) => {
                             log::error!("Rust: Failed to convert batch to PubsubMessages: {:?}", e);
                             return -3;
                         }
                    };

                    let res = writer.rt.block_on(async {
                         writer.client.publish_batch(msgs).await
                    });
                    
                    if let Err(e) = res {
                        log::error!("Rust: Failed to publish batch: {:?}", e);
                        return -4;
                    }
                    1
                }
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
    /// Reference to global Tokio runtime.
    rt: &'static Runtime,
    /// Pub/Sub client instance.
    client: PubSubClient,
    /// Optional Arrow schema for structured parsing.
    schema: Option<arrow::datatypes::SchemaRef>,
    /// Format of the data in Pub/Sub (JSON or Avro).
    format: crate::arrow_convert::DataFormat,
    /// Optional Avro schema for Avro parsing.
    avro_schema: Option<apache_avro::Schema>,
}

/// Internal state for a Spark partition writer, including its dedicated Tokio runtime.
/// 
/// This struct holds the resources needed to publish messages to Pub/Sub.
pub struct RustPartitionWriter {
    /// Reference to global Tokio runtime.
    rt: &'static Runtime,
    /// Pub/Sub publisher client instance.
    client: crate::pubsub::PublisherClient,
}
