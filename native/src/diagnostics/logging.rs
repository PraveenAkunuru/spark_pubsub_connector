//! # Native JNI Logging Bridge
//!
//! This module provides a `log` facade implementation that forwards Rust logs
//! back to Spark's `NativeLogger` using the JNI.
//!
//! It uses a background thread and a multi-producer single-consumer (MPSC) channel
//! to decouple Rust execution from JNI/JVM latency.

// 
use log::{Level, Log, Metadata, Record};
use std::sync::{Once, OnceLock};
use std::thread;
use std::sync::mpsc::SyncSender;

// 
use std::cell::RefCell;

static SENDER: OnceLock<SyncSender<(Level, String)>> = OnceLock::new();
static DROPPED_LOGS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    static LOG_CONTEXT: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Sets the current thread's logging context (e.g., "[Partition: 0, Batch: 1]").
pub fn set_context(context: &str) {
    LOG_CONTEXT.with(|c| {
        *c.borrow_mut() = context.to_string();
    });
}

/// Logger implementation that sends records to a background JNI thread.
struct JniLogger;

impl Log for JniLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true // we filter via set_max_level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            if let Some(tx) = SENDER.get() {
                let context = LOG_CONTEXT.with(|c| c.borrow().clone());
                let msg = if context.is_empty() {
                    record.args().to_string()
                } else {
                    format!("{} {}", context, record.args())
                };
                // Use try_send to avoid blocking if the channel is full
                if tx.try_send((record.level(), msg)).is_err() {
                    DROPPED_LOGS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    fn flush(&self) {}
}

const LOGGER_CLASS: &str = "com/google/cloud/spark/pubsub/diagnostics/NativeLogger$";
const LOGGER_SIG: &str = "Lcom/google/cloud/spark/pubsub/diagnostics/NativeLogger$;";

/// Initialize the JNI Logger.
/// Spawns a background thread that attaches to the JVM and calls `NativeLogger.log()`.
pub fn init(env: &jni::JNIEnv) {
    static START: Once = Once::new();
    START.call_once(|| {
        if let Err(e) = init_internal(env) {
            eprintln!("Rust Logging: Initialization failed: {:?}", e);
        }
    });
}

fn init_internal(env: &jni::JNIEnv) -> Result<(), Box<dyn std::error::Error>> {
    // Increased buffer size to 20k to handle high-throughput bursts
    let (tx, rx) = std::sync::mpsc::sync_channel::<(Level, String)>(20000);

    let (level_filter, is_debug) = match std::env::var("RUST_LOG").ok().as_deref() {
        Some("error") => (log::LevelFilter::Error, false),
        Some("warn") => (log::LevelFilter::Warn, false),
        Some("info") => (log::LevelFilter::Info, false),
        Some("debug") => (log::LevelFilter::Debug, true),
        Some("trace") => (log::LevelFilter::Trace, true),
        _ => (log::LevelFilter::Info, false),
    };

    // Note: We MUST set the logger before we might start using it.
    // However, set_logger can only be called ONCE per process.
    if let Err(e) = log::set_logger(&JniLogger) {
         // If already set, we just continue (unlikely in this architecture but safe)
         eprintln!("Rust Logging: Logger already set: {:?}", e);
    }
    log::set_max_level(level_filter);

    if SENDER.set(tx).is_err() {
        return Err("SENDER already initialized".into());
    }

    // Capture JavaVM to attach background thread
    let vm = env.get_java_vm()?;

    // Find the logger class and create a GlobalRef.
    let logger_class = env.find_class(LOGGER_CLASS)?;
    let logger_class_global = env.new_global_ref(logger_class)?;

    // Get the singleton MODULE$ field
    let module_field = env.get_static_field_id(LOGGER_CLASS, "MODULE$", LOGGER_SIG)?;
    let jval = env.get_static_field_unchecked(LOGGER_CLASS, module_field, jni::signature::JavaType::Object(LOGGER_SIG.to_string()))?;
    
    let logger_obj = match jval {
        jni::objects::JValue::Object(obj) => obj,
        _ => return Err("Failed to get MODULE$ object".into()),
    };
    
    if logger_obj.is_null() {
         return Err("NativeLogger MODULE$ is NULL".into());
    }

    let logger_obj_global = env.new_global_ref(logger_obj)?;

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

        let log_method = match env.get_method_id(&logger_class_global, "log", "(ILjava/lang/String;)V") {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Rust Logging: Failed to get log method in background thread: {:?}", e);
                return;
            }
        };

        // Keep track of JNI failures
        let mut failure_count = 0;

        while let Ok((level, msg)) = rx.recv() {
            let lvl_int = match level {
                Level::Error => 1,
                Level::Warn => 2,
                Level::Info => 3,
                Level::Debug => 4,
                Level::Trace => 5,
            };

            let jmsg = match env.new_string(&msg) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let res = env.call_method_unchecked(
                &logger_obj_global,
                log_method,
                jni::signature::JavaType::Primitive(jni::signature::Primitive::Void),
                &[
                    jni::objects::JValue::Int(lvl_int),
                    jni::objects::JValue::Object(jmsg.into()),
                ],
            );

            if res.is_err() || env.exception_check().unwrap_or(false) {
                failure_count += 1;
                if failure_count < 10 {
                    eprintln!("Rust Logging: JNI call failed (count: {}). Clearing exception.", failure_count);
                    let _ = env.exception_describe();
                    let _ = env.exception_clear();
                }

                // In debug mode, fallback to console if JNI fails
                if is_debug {
                    eprintln!("[NATIVE-{:?}] {}", level, msg);
                }
            } else {
                failure_count = 0; // Reset on success
            }
        }
    });

    eprintln!("Rust: JNI Logging initialized via GlobalRef.");
    Ok(())
}
