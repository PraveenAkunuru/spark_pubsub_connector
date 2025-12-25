# Spark-to-Pub/Sub Write Path (Sink) Test Plan

## 1. Sink (Write Path) Test Plan Table

| Scenario Category | Test Objective | Source Requirement | Key Configuration |
| :--- | :--- | :--- | :--- |
| **Functional Integrity** | Verify that 100% of rows written from Spark arrive in Pub/Sub accurately. | REQ-SINK-01 | `subscriptionId`, `projectId` |
| **Batching Efficiency** | Confirm Rust triggers a publish call when byte or count thresholds are met. | REQ-SINK-02 | `batchSize`, `lingerTime` |
| **Resilience (Transient)** | Ensure exponential backoff handles `UNAVAILABLE` or `DEADLINE_EXCEEDED`. | REQ-SINK-03 | N/A (Internal Tonic logic) |
| **Resilience (Fatal)** | Verify task failure and micro-batch retry if max retries are exceeded. | REQ-SINK-03 | `ackOnCommit` |
| **Backpressure** | Verify Spark task blocks if internal Rust publisher buffers are saturated. | REQ-NFR-02 | `maxWaitTimeMs` |
| **Memory Safety** | Confirm Arrow release callbacks free off-heap memory after publish. | REQ-NFR-01 | N/A (Arrow C Data Interface) |

## 2. Detailed Test Scenario Specifications

### A. The "Smart Batching" & Linger Test
**Objective**: Validate the `AsyncBatchWrite` implementation logic in the Rust layer.

*   **Procedure**: Configure a `batchSize` of 1000 and a `lingerTime` of 5000ms.
    *   **Sub-test 1 (Count Trigger)**: Write 1000 rows quickly and verify the Rust layer immediately initiates an export to Pub/Sub.
    *   **Sub-test 2 (Time Trigger)**: Write 10 rows and wait. Verify the batch is flushed exactly after 5000ms (the linger interval).
*   **Validation**: Monitor Rust-side metrics to ensure the number of publish calls aligns with batching settings rather than row counts.

### B. At-Least-Once Delivery & Failure Recovery
**Objective**: Ensure the connector maintains data integrity during definitive failures.

*   **Procedure**: Simulate a persistent `PERMISSION_DENIED` error or exceed the maximum retry limit in the Rust tonic client.
*   **Expected Behavior**: The DataWriter must catch the failure and throw a Java exception, causing the Spark task to fail.
*   **Recovery Validation**: Confirm Spark reschedules the task and attempts to re-write the same micro-batch.
*   **Exactly-Once Check**: Verify that users are directed to use `dropDuplicates` or idempotent sinks if they require exactly-once consistency.

### C. FFI Boundary & Memory Leak Stress Test
**Objective**: Validate that the high-speed JNI bridge does not cause "Stop-the-World" GC pauses or OOMs.

*   **Procedure**: Run a continuous write stream (100k msg/sec) for 24+ hours.
*   **Validation**: Monitor the JVM heap usage and native memory (RSS).
*   **Success Criteria**:
    *   JVM Heap must remain stable (not promoted to Old Gen) because the binary payloads reside in off-heap Arrow memory.
    *   Total memory usage should not increase over time, confirming that the `ArrowArray.release` callbacks are firing correctly.

### D. The "Emulator" Integration Test
**Objective**: A prerequisite for all CI/CD pipelines to ensure local development doesn't require live GCP resources.

*   **Setup**: Use the Google Cloud Pub/Sub Emulator in a Docker container.
*   **Configuration**: Set the `emulatorHost` parameter to point the connector to the local container.
*   **Goal**: Perform a complete "Pass-Through" test where Spark writes to a topic and a separate reader (or the same job) verifies the message content and count.
