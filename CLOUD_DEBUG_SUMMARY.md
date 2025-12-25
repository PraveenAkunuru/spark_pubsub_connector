# Cloud Verification Debug Summary: Dataproc 2.3

This document summarizes the issues encountered and the diagnostic steps taken while verifying the Spark Pub/Sub connector on a live Dataproc 2.3 cluster (`cluster-aaf3`).

## Current Status: `init()` returning 0
The native library initializes (`NativeLoader.load()`) successfully, but the `NativeReader.init()` call returns `0` (indicating failure) on both the driver and executors.

## Identified Issues & Resolutions

### 1. TLS Certificate Validation (`UnknownIssuer`)
- **Symptoms**: `ConnectError(Custom { kind: InvalidData, error: InvalidCertificate(UnknownIssuer) })` when connecting to `pubsub.googleapis.com`.
- **Cause**: The default `rustls` configuration in `tonic` failed to discover the system's trust roots on the Debian 12 nodes correctly.
- **Resolution**: Refactored `native/src/pubsub.rs` to explicitly load `/etc/ssl/certs/ca-certificates.crt` during gRPC channel creation.
- **Verification**: "Rust Raw: Connected to gRPC endpoint" log confirmed success.

### 2. Authentication Token Type (`ACCESS_TOKEN_TYPE_UNSUPPORTED`)
- **Symptoms**: `google.rpc.errorinfo-bin: ... ACCESS_TOKEN_TYPE_UNSUPPORTED` from the Pub/Sub backend.
- **Cause**: Default token acquisition did not explicitly include the necessary scopes for Pub/Sub access on restricted worker nodes.
- **Resolution**: Refactored `native/src/pubsub.rs` to explicitly request `https://www.googleapis.com/auth/cloud-platform` and `https://www.googleapis.com/auth/pubsub` scopes.
- **Verification**: "Rust Raw: Acquired token (len=1031)" log confirmed success.

### 3. Subscription Validation Failure (Active Blocker)
- **Symptoms**: `init()` still returns `0`.
- **Suspected Cause**: The call to `client.get_subscription()` in `PubSubClient::new` is failing or timing out.
- **Diagnostic Step**: Added granular `println!` calls to capture the exact gRPC error message from `get_subscription` in standard output (since JNI logging in background threads is currently unreliable).
- **Status**: Waiting for job logs to reveal the specific error (e.g., `PERMISSION_DENIED`, `NOT_FOUND`, or timeout).

### 4. JNI Logging Invisibility
- **Symptoms**: Rust `log::info!` calls from background threads (managed via JNI bridge) are often missing from the Spark driver output.
- **Resolution**: Reverting to raw `println!` for critical diagnostic paths, as Dataproc reliably captures `stdout` in the driver logs.

### 5. Disk Persistence / Node Access
- **Symptoms**: `ExecutorDiag` disk check failed on executors with "Permission denied" for `/tmp/executor_diag_disk_check.txt`.
- **Note**: This may be due to YARN container sandboxing. Driver-side disk check succeeded.

## Next Steps for Review
1. **Analyze logs for `Rust Raw: Subscription validation failed: ...`** to find the exact gRPC status code.
2. **Verify IAM**: Ensure the principal has `pubsub.subscriptions.get` (already granted `roles/pubsub.viewer` which includes this).
3. **Connectivity**: Rule out private Google Access vs. Public DNS resolution for `pubsub.googleapis.com`.

## Build Environment
- **Container**: `rust:bullseye` (required for GLIBC 2.31 compatibility with Dataproc 2.3).
- **Architecture**: `linux-x86-64`.
- **Library Path**: `src/main/resources/linux-x86-64/libnative_pubsub_connector.so`.
