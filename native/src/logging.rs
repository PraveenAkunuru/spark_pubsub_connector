//! # Native JNI Logging Bridge
//!
//! This module provides a `log` facade implementation that forwards Rust logs
//! back to Spark's `NativeLogger` using the JNI.
//!
//! It uses a background thread and a multi-producer single-consumer (MPSC) channel
//! to decouple Rust execution from JNI/JVM latency.

use jni::JNIEnv;
use log::{Level, Log, Metadata, Record};
use std::sync::{Once, OnceLock};
use std::thread;
use tokio::sync::mpsc;

static INIT: Once = Once::new();
static SENDER: OnceLock<mpsc::Sender<(Level, String)>> = OnceLock::new();

/// Logger implementation that sends records to a background JNI thread.
struct JniLogger;

impl Log for JniLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true // we filter via set_max_level
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

const LOGGER_CLASS: &str = "com/google/cloud/spark/pubsub/NativeLogger$";
const LOGGER_SIG: &str = "Lcom/google/cloud/spark/pubsub/NativeLogger$;";

/// Initialize the JNI Logger.
/// Spawns a background thread that attaches to the JVM and calls `NativeLogger.log()`.
pub fn init(env: &JNIEnv) {
    INIT.call_once(|| {
        let (tx, mut rx) = mpsc::channel::<(Level, String)>(10000);

        if SENDER.set(tx).is_err() {
            eprintln!("Rust Logging: Failed to set SENDER OnceLock");
            return;
        }

        log::set_logger(&JniLogger).unwrap();
        let level_filter = match std::env::var("RUST_LOG").ok().as_deref() {
            Some("error") => log::LevelFilter::Error,
            Some("warn") => log::LevelFilter::Warn,
            Some("info") => log::LevelFilter::Info,
            Some("debug") => log::LevelFilter::Debug,
            Some("trace") => log::LevelFilter::Trace,
            _ => log::LevelFilter::Info,
        };
        log::set_max_level(level_filter);

        // Capture JavaVM to attach background thread
        let vm = env.get_java_vm().expect("Failed to get JavaVM");

        // Find the logger class and create a GlobalRef.
        let logger_class = match env.find_class(LOGGER_CLASS) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Rust Logging: NativeLogger class not found: {:?}", e);
                return;
            }
        };
        let logger_class_global = env.new_global_ref(logger_class).expect("Failed to create GlobalRef for logger class");

        // Get the singleton MODULE$ field using the class name string
        let module_field = match env.get_static_field_id(
            LOGGER_CLASS,
            "MODULE$",
            LOGGER_SIG,
        ) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Rust Logging: Failed to get MODULE$: {:?}", e);
                return;
            }
        };

        let logger_obj = match env.get_static_field_unchecked(
            LOGGER_CLASS,
            module_field,
            jni::signature::JavaType::Object(LOGGER_SIG.to_string()),
        ) {
            Ok(jni::objects::JValue::Object(obj)) => obj,
            _ => {
                eprintln!("Rust Logging: Failed to get logger object");
                return;
            }
        };
        
        let logger_obj_global = env.new_global_ref(logger_obj).expect("Failed to create GlobalRef for logger instance");

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

            // Look up the method ID inside the background thread.
            // env.get_method_id is safe here because we have the class GlobalRef.
            let log_method = match env.get_method_id(&logger_class_global, "log", "(ILjava/lang/String;)V")
            {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Rust Logging: Failed to get log method in background thread: {:?}", e);
                    return;
                }
            };

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            
            rt.block_on(async {
                while let Some((level, msg)) = rx.recv().await {
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
                        &logger_obj_global,
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

        eprintln!("Rust: JNI Logging initialized via GlobalRef.");
    });
}
