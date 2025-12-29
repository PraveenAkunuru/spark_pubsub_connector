//! # Native Performance Metrics
//!
//! This module provides atomic counters for tracking throughput, errors, and latency
//! in the native data plane. These metrics are exposed to Spark via JNI for
//! monitoring and observability.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Current bytes held in the asynchronous memory buffer.
pub static BUFFERED_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Total bytes ingested from Pub/Sub since process start.
pub static INGESTED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Total number of messages ingested from Pub/Sub since process start.
pub static INGESTED_MESSAGES: AtomicU64 = AtomicU64::new(0);

/// Total bytes published to Pub/Sub (Sink path).
pub static PUBLISHED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Total messages published to Pub/Sub (Sink path).
pub static PUBLISHED_MESSAGES: AtomicU64 = AtomicU64::new(0);

/// Accumulator for errors encountered during the read (Subscribe) path.
pub static READ_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Accumulator for errors encountered during the write (Publish) path.
pub static WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Count of retries performed by the Pub/Sub clients.
pub static RETRY_COUNT: AtomicU64 = AtomicU64::new(0);

/// Accumulated time spent waiting for publish acknowledgments (microseconds).
pub static PUBLISH_LATENCY_TOTAL_MICROS: AtomicU64 = AtomicU64::new(0);

/// Accumulated time spent waiting for message acknowledgments (microseconds).
pub static ACK_LATENCY_TOTAL_MICROS: AtomicU64 = AtomicU64::new(0);

/* --- Getter Functions for JNI --- */

pub fn get_buffered_bytes() -> i64 { BUFFERED_BYTES.load(Ordering::Relaxed) as i64 }
pub fn get_ingested_bytes() -> i64 { INGESTED_BYTES.load(Ordering::Relaxed) as i64 }
pub fn get_ingested_messages() -> i64 { INGESTED_MESSAGES.load(Ordering::Relaxed) as i64 }
pub fn get_published_bytes() -> i64 { PUBLISHED_BYTES.load(Ordering::Relaxed) as i64 }
pub fn get_published_messages() -> i64 { PUBLISHED_MESSAGES.load(Ordering::Relaxed) as i64 }
pub fn get_read_errors() -> i64 { READ_ERRORS.load(Ordering::Relaxed) as i64 }
pub fn get_write_errors() -> i64 { WRITE_ERRORS.load(Ordering::Relaxed) as i64 }
pub fn get_retry_count() -> i64 { RETRY_COUNT.load(Ordering::Relaxed) as i64 }
pub fn get_publish_latency_micros() -> i64 { PUBLISH_LATENCY_TOTAL_MICROS.load(Ordering::Relaxed) as i64 }
pub fn get_ack_latency_micros() -> i64 { ACK_LATENCY_TOTAL_MICROS.load(Ordering::Relaxed) as i64 }
