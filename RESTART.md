# Project Status: Spark Pub/Sub Connector Debugging

## Current Progress
*   **[FIXED] Write Path Stability**: Resolved the persistent 30s flush timeouts (`NativeWriter.close` failing with code -1).
    *   **Root Cause**: Independent Tokio runtimes were assigned to each partition, but gRPC channels were shared via a pool. When a runtime was dropped, its associated channel's background tasks were killed, causing `BrokenPipe` and transport errors in other partitions.
    *   **Solution**: Implemented a `GLOBAL_RUNTIME` singleton in `native/src/pubsub.rs`. All readers and writers now use this shared, long-lived runtime.
    *   **Status**: `PubSubWriteIntegrationTest` passes consistently.
*   **[FIXED] Logging Visibility**:
    *   Native JNI logger now respects the `RUST_LOG` environment variable.
    *   Critical read-path traces (receipt of messages, passing to Spark) promoted to `INFO` level to bypass Spark log filtering.

## Active Blocker: Read Path Zero-Rows
*   **Problem**: `AckIntegrationTest` reads 0 rows despite confirmed publication.
*   **Native Discovery**: Logs in `write_verify_debug_v6.log` confirm:
    1.  `getNextBatch` is called by Spark.
    2.  `StreamingPull` is established.
    3.  **20 messages are successfully received** and "passed to Spark" via the Arrow FFI boundary.
*   **Remaining Gap**: These 20 messages disappear after the JNI return. Spark's `count()` shows 0.

## Next Steps
1.  **Audit Schema Alignment**: Verify that `ArrowBatchBuilder` field names (`message_id`, `publish_time`, `payload`, `ack_id`) exactly match `PubSubTableProvider.inferSchema` (case sensitivity, nullability).
2.  **Trace Arrow FFI Import**: Check `PubSubPartitionReaderBase.fetchNativeBatch` to see if `Data.importVectorSchemaRoot` is returning a valid root with rows.
3.  **Debug Partition Planning**: Ensure `PubSubMicroBatchStream` isn't accidentally advancing offsets before data is consumed.

## Environment Notes
*   **JDK**: 17 (requires JPMS flags).
*   **Emulator**: `127.0.0.1:8089`.
*   **Logs**: Refer to `write_verify_debug_v7.log` for the latest traces.
*   **Workplace State**: `native/src/lib.rs` and `native/src/pubsub.rs` have return-path traces added for debugging.
