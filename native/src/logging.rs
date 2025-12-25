//! # Native JNI Logging Bridge
//!
//! This module provides a `log` facade implementation that forwards Rust logs
//! back to Spark's `NativeLogger` using the JNI.
//!
//! It uses a background thread and a multi-producer single-consumer (MPSC) channel
//! to decouple Rust execution from JNI/JVM latency.

use jni::objects::AutoLocal;
use jni::JavaVM;
use log::{Level, Log, Metadata, Record};
use std::sync::{Once, OnceLock};
use std::thread;
use tokio::sync::mpsc;

static INIT: Once = Once::new();
static SENDER: OnceLock<mpsc::Sender<(Level, String)>> = OnceLock::new();

/// Logger implementation that sends records to a background JNI thread.
struct JniLogger;

impl Log for JniLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info // Default to Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if let Some(tx) = SENDER.get() {
                // Use try_send to avoid blocking if the channel is full
                let _ = tx.try_send((record.level(), record.args().to_string()));
            }
        }
    }

    fn flush(&self) {}
}

/// Initialize the JNI Logger.
/// Spawns a background thread that attaches to the JVM and calls `NativeLogger.log()`.
pub fn init(vm: JavaVM) {
    INIT.call_once(|| {
        let (tx, mut rx) = mpsc::channel::<(Level, String)>(10000);

        if SENDER.set(tx).is_err() {
            eprintln!("Rust Logging: Failed to set SENDER OnceLock");
            return;
        }

        log::set_logger(&JniLogger).unwrap();
        log::set_max_level(log::LevelFilter::Info);

        // Spawn background thread to drain logs to JNI
        thread::spawn(move || {
            // Attach this thread to the JVM
            let env = match vm.attach_current_thread_permanently() {
                Ok(env) => env,
                Err(e) => {
                    eprintln!("Rust Logging: Failed to attach thread: {:?}", e);
                    return;
                }
            };

            // Resolve class and method ID once
            // We use AutoLocal to satisfy the Desc trait requirements for lookups
            // AutoLocal::new takes (env, obj).
            let logger_class = match env.find_class("com/google/cloud/spark/pubsub/NativeLogger$") {
                Ok(c) => AutoLocal::new(&env, c.into()),
                Err(e) => {
                    eprintln!("Rust Logging: NativeLogger class not found: {:?}", e);
                    return;
                }
            };

            // Get the singleton MODULE$ field
            // Note: AutoLocal implements Desc, so we can pass &logger_class
            let module_field = match env.get_static_field_id(
                &logger_class,
                "MODULE$",
                "Lcom/google/cloud/spark/pubsub/NativeLogger$;",
            ) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Rust Logging: Failed to get MODULE$: {:?}", e);
                    return;
                }
            };

            // JavaType::Object requires the signature string
            let logger_obj = match env.get_static_field_unchecked(
                &logger_class,
                module_field,
                jni::signature::JavaType::Object(
                    "Lcom/google/cloud/spark/pubsub/NativeLogger$;".to_string(),
                ),
            ) {
                Ok(jni::objects::JValue::Object(obj)) => obj,
                _ => {
                    eprintln!("Rust Logging: Failed to get logger object");
                    return;
                }
            };

            let log_method = match env.get_method_id(&logger_class, "log", "(ILjava/lang/String;)V")
            {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Rust Logging: Failed to get log method: {:?}", e);
                    return;
                }
            };

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                while let Some((level, msg)) = rx.recv().await {
                    // level mapping: Error=1, Warn=2, Info=3, Debug=4, Trace=5
                    let lvl_int = match level {
                        Level::Error => 1,
                        Level::Warn => 2,
                        Level::Info => 3,
                        Level::Debug => 4,
                        Level::Trace => 5,
                    };

                    let jmsg = match env.new_string(msg) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    let _ = env.call_method_unchecked(
                        logger_obj,
                        log_method,
                        jni::signature::JavaType::Primitive(jni::signature::Primitive::Void),
                        &[
                            jni::objects::JValue::Int(lvl_int),
                            jni::objects::JValue::Object(jmsg.into()),
                        ],
                    );
                }
            });
        });

        eprintln!("Rust: JNI Logging initialized.");
    });
}
