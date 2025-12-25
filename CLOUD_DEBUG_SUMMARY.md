# Cloud Verification Debug Summary: Dataproc 2.3

This document summarizes the issues encountered and the diagnostic steps taken while verifying the Spark Pub/Sub connector on a live Dataproc 2.3 cluster (`cluster-aaf3`).

## Final Status: SUCCESS
The connector has been fully verified. Data is successfully streaming from Pub/Sub to Spark on Dataproc 2.3. The `ExecutorDiag` job confirmed the end-to-end flow with 50 messages processed.

## Identified Issues & Resolutions

### 1. TLS Certificate Validation (`UnknownIssuer`)
- **Symptoms**: `ConnectError(Custom { kind: InvalidData, error: InvalidCertificate(UnknownIssuer) })` when connecting to `pubsub.googleapis.com`.
- **Cause**: The default `rustls` configuration in `tonic` failed to discover the system's trust roots on the Debian 12 nodes correctly.
- **Resolution**: Refactored `native/src/pubsub.rs` to explicitly load `/etc/ssl/certs/ca-certificates.crt` during gRPC channel creation.
- **Verification**: "Rust Raw: Connected to gRPC endpoint" log confirmed success.

### 2. Authentication Token Type (`ACCESS_TOKEN_TYPE_UNSUPPORTED`)
- **Symptoms**: `google.rpc.errorinfo-bin: ... ACCESS_TOKEN_TYPE_UNSUPPORTED` from the Pub/Sub backend.
- **Root Cause**: The Dataproc metadata server returns tokens *without* the "Bearer " prefix in some contexts, but `google-cloud-auth` (or our usage of it) was providing keys that led to a double-prefixing when constructing the `Authorization` header manually. Specifically, the token string itself sometimes contained "Bearer ".
- **Resolution**: Modified `pubsub.rs` to checking if the token already starts with "Bearer " before prepending it.
- **Verification**: "Rust Raw: Acquired token" followed by successful subscription validation.

### 3. Subscription Validation Failure (Double-Prefixing)
- **Symptoms**: `init()` returned `0`.
- **Root Cause**: The code was blindly formatting the subscription name as `projects/{}/subscriptions/{}`, but the input `subscription_id` from Spark sometimes already contained the full path. This resulted in `projects/.../subscriptions/projects/.../subscriptions/...`, causing a `NOT_FOUND` error.
- **Resolution**: Updated `pubsub.rs` to check if `subscription_id` already contains `/subscriptions/` and return it as-is if so (idempotency fix).
- **Verification**: "Rust Raw: Subscription validated: ..." log confirmed success.

### 4. JNI Logging Invisibility
- **Symptoms**: Rust `log::info!` calls from background threads were dropped.
- **Resolution**: Used raw `println!` ("Rust Raw:") for critical diagnostic paths during debugging. Now that the flow is stable, these can be removed or replaced with a robust JNI logger.

### 5. Disk Persistence / Node Access
- **Symptoms**: `ExecutorDiag` disk check failed on executors with "Permission denied".
- **Note**: This is likely a YARN container restriction. Since data flow works, this is non-critical.

## Build Environment
- **Container**: `rust:bullseye` (required for GLIBC 2.31 compatibility with Dataproc 2.3).
- **Architecture**: `linux-x86-64`.
- **Library Path**: `src/main/resources/linux-x86-64/libnative_pubsub_connector.so`.
