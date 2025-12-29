//! # Core Connector Infrastructure
//!
//! This module provides the central infrastructure for the Pub/Sub connector,
//! including the shared Tokios runtime, Pub/Sub client management, and metrics.

pub mod client;
pub mod metrics;
pub mod runtime;
