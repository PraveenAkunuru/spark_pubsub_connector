//! # Native JNI Logging Bridge
//!
//! This module provides a `log` facade implementation that forwards Rust logs
//! back to Spark's `NativeLogger` using the JNI.
//!
//! It uses a background thread and a multi-producer single-consumer (MPSC) channel
//! to decouple Rust execution from JNI/JVM latency.

use log::{Level, Log, Metadata, Record};
use std::sync::{Once, OnceLock};
use std::thread;
use std::sync::mpsc::SyncSender;
use std::cell::RefCell;

/// Global sender for the logging channel.
static SENDER: OnceLock<SyncSender<(Level, String)>> = OnceLock::new();

/// Counter for logs dropped due to channel congestion.
static DROPPED_LOGS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    /// Thread-local storage for logging context (e.g., partition ID).
    static LOG_CONTEXT: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Sets the current thread's logging context.
/// This context is prefixed to every log message generated on this thread.
pub fn set_context(context: &str) {
    LOG_CONTEXT.with(|c| {
        *c.borrow_mut() = context.to_string();
    });
}

/// Logger implementation that sends records to a background JNI thread.
struct JniLogger;

impl Log for JniLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        // Filtering is handled via log::set_max_level during initialization.
        true
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
                // Use try_send to avoid blocking native data plane threads if JVM logging stalls.
                if tx.try_send((record.level(), msg)).is_err() {
                    DROPPED_LOGS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    fn flush(&self) {}
}

const LOGGER_CLASS: &str = "finalconnector/NativeLogger$";
const LOGGER_SIG: &str = "Lfinalconnector/NativeLogger$;";

/// Initializes the JNI Logger.
///
/// This function is idempotent and should be called once per executor process
/// or from every JNI entry point to ensure logs are captured.
/// It spawns a background thread that maintains a permanent JNI attachment to the JVM.
pub fn init(env: &jni::JNIEnv) {
    static START: Once = Once::new();
    START.call_once(|| {
        if let Err(e) = init_internal(env) {
            eprintln!("Rust Logging: Initialization failed: {:?}", e);
        }
    });
}

fn init_internal(env: &jni::JNIEnv) -> Result<(), Box<dyn std::error::Error>> {
    // 20,000 message buffer provides ~20MB of log burst protection.
    let (tx, rx) = std::sync::mpsc::sync_channel::<(Level, String)>(20000);

    let (level_filter, is_debug) = match std::env::var("RUST_LOG").ok().as_deref() {
        Some("error") => (log::LevelFilter::Error, false),
        Some("warn") => (log::LevelFilter::Warn, false),
        Some("info") => (log::LevelFilter::Info, false),
        Some("debug") => (log::LevelFilter::Debug, true),
        Some("trace") => (log::LevelFilter::Trace, true),
        _ => (log::LevelFilter::Info, false),
    };

    if let Err(e) = log::set_logger(&JniLogger) {
         eprintln!("Rust Logging: Logger already set: {:?}", e);
    }
    log::set_max_level(level_filter);

    if SENDER.set(tx).is_err() {
        return Err("SENDER already initialized".into());
    }

    let vm = env.get_java_vm()?;
    let logger_class = env.find_class(LOGGER_CLASS)?;
    let logger_class_global = env.new_global_ref(logger_class)?;

    // Retrieve the Scala singleton MODULE$ instance.
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

    thread::spawn(move || {
        let env = match vm.attach_current_thread_permanently() {
            Ok(env) => env,
            Err(e) => {
                eprintln!("Rust Logging: Failed to attach thread: {:?}", e);
                return;
            }
        };

        // Scala NativeLogger.log(level: Int, msg: String)
        let log_method = match env.get_method_id(&logger_class_global, "log", "(ILjava/lang/String;)V") {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Rust Logging: Failed to get log method in background thread: {:?}", e);
                return;
            }
        };

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

                if is_debug {
                    eprintln!("[NATIVE-{:?}] {}", level, msg);
                }
            } else {
                failure_count = 0;
            }
        }
    });

    eprintln!("Rust: JNI Logging bridge active.");
    Ok(())
}
