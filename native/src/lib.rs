use robusta_jni::bridge;

mod pubsub;
mod arrow_convert;

use pubsub::PubSubClient;
use tokio::runtime::Runtime;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

// ABI-compatible mirrors for Arrow FFI structures. 
// These are used to access private fields or to implement "Peek" mode (noop release).
#[repr(C)]
pub struct FFI_ArrowSchema_Mirror {
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
pub struct FFI_ArrowArray_Mirror {
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

#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#[bridge]
mod jni {
    use crate::pubsub::PubSubClient;
    use crate::arrow_convert::ArrowBatchBuilder;
    use robusta_jni::convert::{Signature, IntoJavaValue, FromJavaValue, TryIntoJavaValue, TryFromJavaValue};
    use robusta_jni::jni::JNIEnv;
    use robusta_jni::jni::objects::AutoLocal;
    use robusta_jni::jni::sys::jlong;
    use tokio::runtime::Runtime;
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::array::{Array, StructArray};
    
    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue, FromJavaValue)]
    #[package(com.google.cloud.spark.pubsub)]
    pub struct NativeReader<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> NativeReader<'env, 'borrow> {
        
        pub extern "jni" fn init(self, _env: &JNIEnv, project_id: String, subscription_id: String) -> jlong {
            let rt = Runtime::new().expect("Failed to create Tokio runtime");
            
            let client_res = rt.block_on(async {
                PubSubClient::new(&project_id, &subscription_id).await
            });

            match client_res {
                Ok(c) => {
                    let reader = Box::new(crate::RustPartitionReader {
                        rt,
                        client: c,
                    });
                    Box::into_raw(reader) as jlong
                },
                Err(e) => {
                    eprintln!("Rust: Failed to initialize reader: {:?}", e);
                    0
                }
            }
        }

        pub extern "jni" fn getNextBatch(self, _env: &JNIEnv, reader_ptr: jlong, out_array: jlong, out_schema: jlong) -> i32 {
            if reader_ptr == 0 {
                return -1;
            }
            // SAFETY: Java side provides valid pointers to FFI structures
            let reader = unsafe { &mut *(reader_ptr as *mut crate::RustPartitionReader) };
            
            let msgs = reader.rt.block_on(async {
                reader.client.fetch_batch(1000).await
            });
            
            if msgs.is_empty() {
                return 0;
            }

            let mut builder = ArrowBatchBuilder::new();
            for msg in msgs {
                builder.append(&msg);
            }
            
            let (arrays, schema) = builder.finish();
            
            let batch = match arrow::record_batch::RecordBatch::try_new(schema, arrays) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Rust: Failed to create RecordBatch: {:?}", e);
                    return -2;
                }
            };
            
            // Convert to FFI structs via StructArray
            let struct_array = StructArray::from(batch);
            let (array_val, schema_val) = match arrow::ffi::to_ffi(&struct_array.into_data()) {
                Ok(ffi) => ffi,
                Err(e) => {
                    eprintln!("Rust: Failed to export to FFI: {:?}", e);
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
            
            1
        }

        pub extern "jni" fn close(self, _env: &JNIEnv, reader_ptr: jlong) {
            if reader_ptr != 0 {
                let _ = unsafe { Box::from_raw(reader_ptr as *mut crate::RustPartitionReader) };
            }
        }
    }

    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue, FromJavaValue)]
    #[package(com.google.cloud.spark.pubsub)]
    pub struct NativeWriter<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> NativeWriter<'env, 'borrow> {
        pub extern "jni" fn init(self, _env: &JNIEnv, project_id: String, topic_id: String) -> jlong {
            let rt = Runtime::new().expect("Failed to create Tokio runtime for writer");
            
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
                    eprintln!("Rust: Failed to define publisher: {:?}", e);
                    0
                }
            }
        }

        pub extern "jni" fn writeBatch(self, _env: &JNIEnv, writer_ptr: jlong, arrow_array_addr: jlong, arrow_schema_addr: jlong) -> i32 {
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

                // 1. Validate both Schema and Array structures before taking ownership
                let schema_val_raw = std::ptr::read(schema_ptr);
                let schema_mirror: crate::FFI_ArrowSchema_Mirror = std::mem::transmute(schema_val_raw);
                
                if schema_mirror.n_children > 0 {
                    if schema_mirror.children.is_null() {
                        eprintln!("Rust: FFI Error - Schema n_children is {} but children pointer is NULL", schema_mirror.n_children);
                        return -20;
                    }
                    for i in 0..schema_mirror.n_children {
                        let child_ptr = *schema_mirror.children.add(i as usize);
                        if child_ptr.is_null() {
                            eprintln!("Rust: FFI Error - Schema child[{}] pointer is NULL", i);
                            return -21;
                        }
                    }
                }

                let array_val_raw = std::ptr::read(array_ptr);
                let array_mirror: crate::FFI_ArrowArray_Mirror = std::mem::transmute(array_val_raw);
                if array_mirror.n_children > 0 {
                    if array_mirror.children.is_null() {
                        eprintln!("Rust: FFI Error - Array n_children is {} but children pointer is NULL", array_mirror.n_children);
                        return -30;
                    }
                    for i in 0..array_mirror.n_children {
                        let child_ptr = *array_mirror.children.add(i as usize);
                        if child_ptr.is_null() {
                            eprintln!("Rust: FFI Error - Array child[{}] pointer is NULL", i);
                            return -31;
                        }
                    }
                }

                // 2. MOVE mode: Take full ownership from Java via the C Data Interface.
                // We already have the raw values. 
                // CRITICAL FIX (Phase 5.6): We DO NOT zero out source pointers.
                // Java Arrow export increments ref counts. Rust import takes one ref.
                // Rust release + Java close = Balanced release.
                // Zeroing out causes panic/crash due to corruption of Java-side structures.
                
                // Transmute back to FFI structs for arrow-rs
                let array_val_final: FFI_ArrowArray = std::mem::transmute(array_mirror);
                let schema_val_final: FFI_ArrowSchema = std::mem::transmute(schema_mirror);

                // 3. Import into arrow-rs
                // from_ffi takes the raw structures and handles the release callback when dropped.
                let array_data = match arrow::ffi::from_ffi(array_val_final, &schema_val_final) {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("Rust: Failed to import Arrow array from FFI: {:?}", e);
                        return -2;
                    }
                };
                
                let array = StructArray::from(array_data);
                
                // Convert to PubsubMessages
                let reader = crate::arrow_convert::ArrowBatchReader::new(&array);
                let msgs = match reader.to_pubsub_messages() {
                     Ok(m) => m,
                     Err(e) => {
                         eprintln!("Rust: Failed to convert batch to PubsubMessages: {:?}", e);
                         return -3;
                     }
                };

                let res = writer.rt.block_on(async {
                     writer.client.publish_batch(msgs).await
                });
                
                if let Err(e) = res {
                    eprintln!("Rust: Failed to publish batch: {:?}", e);
                    return -4;
                }
               
                // Rust's array_data/array will be dropped at the end of this block.
                // Because we set noop_release, it won't trigger any callback to Java.
                // This is safe because the JNI call is synchronous.
            }
            1 // Success
        }



        pub extern "jni" fn close(self, _env: &JNIEnv, writer_ptr: jlong) {
            if writer_ptr != 0 {
                let writer = unsafe { Box::from_raw(writer_ptr as *mut crate::RustPartitionWriter) };
                // Runtime will be dropped here, shutting down connections
                writer.rt.block_on(async {
                    writer.client.flush().await;
                });
            }
        }
    }
}



pub struct RustPartitionReader {
    rt: Runtime,
    client: PubSubClient,
}

pub struct RustPartitionWriter {
    rt: Runtime,
    client: crate::pubsub::PublisherClient,
}
