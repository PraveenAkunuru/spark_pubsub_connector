//! Global asynchronous runtime management.
//!
//! This module provides a singleton Tokio runtime used for all asynchronous
//! operations (Pub/Sub pulls, JNI async orchestration) to ensure efficient
//! resource sharing and prevent thread explosion in the JVM.

use tokio::runtime::Runtime;
use std::sync::OnceLock;

static GLOBAL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Returns a reference to the global Tokio runtime, initializing it if necessary.
///
/// The runtime is configured with a number of worker threads proportional to the
/// available CPU cores, clamped between 16 and 128 to balance throughput and
/// overhead in a specialized JNI context.
pub fn get_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(32)
            .clamp(16, 128);
            
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(threads)
            .thread_name("pubsub-native-worker")
            .build()
            .expect("Failed to create global Tokio runtime")
    })
}
