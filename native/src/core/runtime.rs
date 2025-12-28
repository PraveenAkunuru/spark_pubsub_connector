use tokio::runtime::Runtime;
use std::sync::OnceLock;

static GLOBAL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn get_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(32)
            .max(16)
            .min(128);
            
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(threads)
            .thread_name("pubsub-native-worker")
            .build()
            .expect("Failed to create global Tokio runtime")
    })
}
