# Feature Design: Ack Scalability & Schema Metadata

**Date:** December 2024
**Status:** Verified (2024-12-24)
**Context:** Hardening Phase & Feature Gap Closure

## 1. Acknowledgment Scalability

### Problem
In the original `PubSubMicroBatchStream` implementation, the `pendingCommits` set (tracking confirmed batch IDs to be acked) was a `HashSet` that grew indefinitely. Ensure-once delivery requires retaining these IDs to propagate them to executors, but keeping *all* history leads to unbounded Driver memory growth over time.

### Solution: Time-To-Live (TTL) Propagation
Instead of permanent retention, we implement a **TTL-based** approach for `pendingCommits`.

*   **Mechanism**: `pendingCommits` is converted to a `Map[BatchID, RemainingCycles]`.
*   **Default TTL**: 5 Micro-Batch Cycles.
*   **Logic**:
    1.  When a batch is committed (`commit()`), it is added to the map with `TTL = 5`.
    2.  During `planInputPartitions` (start of next batch):
        *   The list of *active* Batch IDs (TTL > 0) is broadcast to all executors via the `PubSubInputPartition` definition.
        *   The TTL for each ID in the map is decremented by 1.
        *   IDs with `TTL <= 0` are removed from the Driver's map.
*   **Rationale**: 
    *   Spark executors are ephemeral but usually stable within a few scheduling cycles. 
    *   5 cycles provide a robust buffer for all active executors to receive the "Ack Signal" and flush their local reservoirs.
    *   If an executor is down for >5 cycles, it likely lost its in-memory reservoir anyway (crash/restart), so sending the Ack Signal is moot.
    *   This bounds Driver memory usage to `O(BatchRate * 5)`.

## 2. Schema Metadata (Message Attributes)

### Problem
The `PubsubMessage` payload was exposed, but `attributes` (key-value pairs) were dropped during Arrow conversion, limiting the connector's utility for metadata-heavy use cases.

### Solution: Arrow Map Type
We map Pub/Sub attributes to the **Arrow Map** type (`Map<Utf8, Utf8>`).

*   **Schema Definition**:
    ```rust
    Field::new("attributes", DataType::Map(
        Arc::new(Field::new("entries", DataType::Struct(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ].into()), false)),
        false
    ), false)
    ```
*   **Implementation**: 
    *   Uses `arrow::array::MapBuilder` (or generic equivalent) in `arrow_convert.rs`.
    *   Iterates over `ReceivedMessage.message.attributes` to populate the map for each row.
    *   Ensures zero-copy or efficient transfer where possible, though Map construction involves some overhead.

## 3. Configuration Hardening

### Issue
Connection storms during executor startup caused thundering herd issues on the Pub/Sub API.

### Solution: Configurable Jitter
*   **Parameter**: `spark.pubsub.jitter.ms` (Default: 500ms).
*   **Flow**: 
    *   Read from `SparkConf`/Options in `PubSubPartitionReader`.
    *   Passed to `NativeReader.init` via JNI.
    *   Rust uses `rand::gen_range(0..jitter)` to sleep before establishing gRPC connections.

## 4. Verification Outcome

**Date:** 2024-12-24
**Result:** PASSED

### Tests Performed
1.  **Ack Scalability (TTL)**
    *   **Method:** `run_scale_test.sh` (10,000 messages, multiple batches).
    *   **Observation:** Driver logs confirmed batch commits. No memory growth warning. TTL logic correctly pruned old batch IDs after 5 cycles.
2.  **Schema Metadata (Attributes)**
    *   **Method:** `Code verification` + `Arrow Integration`.
    *   **Observation:** Arrow schema successfully negotiated with Map type. `ArrowBatchBuilder` compilation successful with `MapBuilder` fix for borrow checker.
3.  **Configurable Jitter**
    *   **Method:** Integration Test (`NativeIntegrationTest`) and Scale Test.
    *   **Observation:** Initial test failure caught missing `jitterMillis` arg. Fixed signature. Rerun confirmed `init` accepts param and connects successfully.

### Conclusion
The features are stable and integrated into the `spark-pubsub-connector` v0.1.0 codebase.

