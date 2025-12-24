Technical Product Requirements Document: High-Performance Google Cloud Pub/Sub Connector for Apache Spark via Rust and Apache Arrow
1. Executive Summary
This document serves as the comprehensive Technical Product Requirements Document (PRD) and architectural specification for the development of a high-performance, native Google Cloud Pub/Sub connector for Apache Spark. The central innovation of this connector is the replacement of the traditional Java-based gRPC data plane with a Rust-based runtime, integrated via the Java Native Interface (JNI) and the Apache Arrow C Data Interface.
The primary objective is to solve the specific performance bottlenecks associated with high-throughput streaming ingestion on the Java Virtual Machine (JVM). Traditional connectors often suffer from substantial Garbage Collection (GC) pauses when processing millions of small, ephemeral message objects (Protocol Buffers). By offloading network I/O, Protocol Buffer deserialization, and memory management to Rust, and exposing data to Spark strictly as columnar Arrow batches, this architecture aims to achieve near-zero-copy data transfer, significantly higher throughput per core, and deterministic latency profiles required for strict Service Level Agreements (SLAs).
This report details the architectural requirements, implementation specifics for the Read and Write paths, memory safety protocols across the Foreign Function Interface (FFI), failure recovery mechanisms, and a rigorous testing strategy. It is intended for use by Senior Data Engineers and Systems Programmers to guide the end-to-end development lifecycle.

2. Strategic Rationale and Performance Theory
2.1 The JVM Overhead Problem in Streaming
In standard Apache Spark connectors (e.g., Kafka or existing Pub/Sub connectors), data ingestion follows a path that is hostile to the JVM's generational garbage collector:
Network I/O: Bytes are read from a socket into a direct byte buffer.
Deserialization: The gRPC library decodes these bytes into ephemeral Java objects (e.g., PubsubMessage).
Transformation: These objects are often converted into Spark Row objects.
Short Lifespan: These millions of objects live for mere milliseconds before being discarded, saturating the Eden space and triggering frequent Minor GCs. If ingestion rates spike, objects may be promoted to Old Gen, leading to unpredictable "Stop-the-World" Major GC pauses that induce latency spikes.
2.2 The Rust and Arrow Solution
Rust offers memory safety without a garbage collector. By handling the "hot loop" of data ingestion in Rust:
Off-Heap Management: Incoming messages are allocated on the native heap, invisible to the JVM GC.
Vectorization: Messages are immediately parsed into columnar Arrow buffers (e.g., a contiguous block of memory for timestamps, another for payloads).
Zero-Copy Transfer: Instead of copying data into the JVM, the connector passes pointers to these Arrow buffers using the C Data Interface. The JVM wraps these pointers in VectorSchemaRoot objects. The data never effectively enters the JVM heap until Spark's Catalyst engine processes it, often retaining it in off-heap Tungsten format.
The analysis suggests this architecture effectively decouples ingestion throughput from JVM GC pressure, allowing Spark Executors to scale based on CPU compute rather than memory management overhead.

3. Architectural Design
3.1 System Components
The connector architecture is bifurcated into a Control Plane (Scala/Java) and a Data Plane (Rust).
3.1.1 Control Plane (Scala/Java)
This layer integrates with Spark's DataSourceV2 API. It is responsible for:
Query Planning: Negotiating schemas and partition planning with the Spark Driver.
Checkpointing: Managing offsets and committing transaction batches.
Lifecycle Management: Initializing and terminating the Rust runtime on Executors.
Metric Aggregation: Exposing Rust-side metrics (throughput, latency) to Spark's monitoring system.
3.1.2 Data Plane (Rust)
This layer executes the heavy lifting. Implemented as a dynamic shared library (.so / .dylib), it contains:
Async Runtime: A tokio runtime to handle non-blocking gRPC calls.
gRPC Client: A high-performance client (via tonic) interacting with Google Cloud Pub/Sub.
Arrow Builder: Logic to aggregate row-based Pub/Sub messages into columnar Arrow arrays.
JNI Bridge: A strict FFI layer that handles the exchange of memory pointers and manages lifecycle callbacks to prevent memory leaks.
3.2 Data Flow Diagram (Conceptual)
Driver: Calls MicroBatchStream.planInputPartitions(). Calculates how many parallel readers are needed (e.g., 1 per Executor Core).
Executor: Instantiates RustPartitionReader.
Executor (JNI): Calls native_init(subscription_id).
Rust: Spawns tokio background tasks to initiate StreamingPull.
Rust: Buffers incoming messages into ArrowArray structs.
Executor: Calls reader.next().
Rust: Returns pointers to the filled ArrowArray.
Executor: Wraps pointers in VectorSchemaRoot, iterates as InternalRow for Spark.
Executor: Spark processes the batch.
Executor: VectorSchemaRoot is closed; standard Arrow release callback fires, freeing Rust memory.

4. Detailed Product Requirements (PRD)
4.1 Functional Requirements: The Read Path (Source)
REQ-SRC-01: Implementation of TableProvider and DataSourceV2
The connector must implement the TableProvider interface to support the spark.readStream.format(...) syntax. It must negotiate the schema effectively. Since Pub/Sub messages are technically schemaless (binary blobs), the connector must support two modes:
Raw Mode: A fixed schema containing message_id (String), publish_time (Timestamp), attributes (Map<String, String>), and payload (Binary).
Schema Mode: Accepting a user-defined schema (JSON or DDL) and performing projection/decoding within Rust. If spark.readStream.schema(...) is provided, Rust should attempt to decode the JSON/Avro payload immediately into Arrow columns, discarding malformed records or routing them to a Dead Letter Queue (DLQ).
REQ-SRC-02: Partition Planning Strategy
Unlike Kafka, Pub/Sub does not have static partitions. The MicroBatchStream must implement a "Virtual Partitioning" strategy.
The connector shall allow configuration of numPartitions (defaulting to spark.default.parallelism or total executor cores).
Each Spark Partition will correspond to an independent StreamingPull connection in Rust.
This leverages Pub/Sub's inherent server-side load balancing, where the service distributes messages across all active streaming pull connections for a subscription.
REQ-SRC-03: Offset Management (The "Reservoir" Pattern)
Pub/Sub uses ephemeral ack_ids rather than sequential offsets. Spark Structured Streaming requires sequential offsets for fault tolerance. The connector must implement a synthetic offset mechanism:
Start Offset: A logical marker (e.g., Timestamp or a generic "Earliest").
End Offset: The connector must utilize a "Reservoir" approach. As messages are pulled in Rust, they are kept in a "Pending Ack" state in memory. The "Offset" returned to the Driver for a batch is a unique Batch ID or high-watermark timestamp.
Commit Protocol: When MicroBatchStream.commit(endOffset) is invoked by the Driver (signaling successful processing), the connector must propagate this signal to all Executors to Ack the messages held in the reservoir.
REQ-SRC-04: Late Data Handling
The connector must correctly extract publish_time from the Pub/Sub metadata to support Spark's Event Time Watermarking.
The Rust layer must ensure publish_time is accurately converted to Arrow's Timestamp(Microsecond) type.
The connector must NOT drop late data itself; it must pass all available data to Spark. Spark's withWatermark logic will handle the dropping of data older than the threshold during stateful aggregations.
4.2 Functional Requirements: The Write Path (Sink)
REQ-SINK-01: SupportsWrite Implementation
The connector must implement SupportsWrite to allow df.writeStream.format(...).start().
The implementation must use AsyncBatchWrite semantics.
The DataWriter (running on Executor) must instantiate a Rust-based Publisher.
REQ-SINK-02: Batching and Throughput
Writing individual messages is inefficient. The Rust layer must implement smart batching:
Batch Size: Configurable by byte size (e.g., 5MB) or message count (e.g., 1000).
Linger Time: Configurable latency (e.g., 5ms) to wait for a batch to fill.
This ensures optimal utilization of the Pub/Sub Publish API, which supports batching natively.
REQ-SINK-03: Error Handling and Retries
The Rust Publisher must handle transient errors (e.g., UNAVAILABLE, DEADLINE_EXCEEDED) using exponential backoff without blocking the Spark Executor thread if possible (using async await).
If a message definitively fails (e.g., PERMISSION_DENIED or max retries exceeded), the DataWriter must throw an exception to fail the Spark task, triggering a retry of the micro-batch to ensure At-Least-Once consistency.
4.3 Non-Functional Requirements
REQ-NFR-01: Memory Safety & Zero Leaks
The bridge must guarantee that every memory allocation passed to Java has a corresponding release callback.
The implementation must effectively use the ArrowArray.release callback to drop the Rust Arc<T> holding the data.
Extensive automated testing must verify memory usage remains stable over long-running streams (24+ hours).
REQ-NFR-02: Latency
The overhead of the JNI call and Arrow conversion must be < 5ms per batch. The end-to-end latency (ingest to process) should be primarily dictated by the micro-batch interval, not connector overhead.
REQ-NFR-03: Dependency Isolation
The Rust shared library must be statically linked (except for libc) to avoid "DLL Hell" on target Executors. It must run reliably on standard Linux distributions (Debian/Ubuntu, CentOS/RHEL) commonly used in Dataproc and Kubernetes images.

5. Technical Implementation Specification
5.1 The Arrow C Data Interface Integration
The core of the high-speed interface is the FFI_ArrowArray struct.
Rust Side (Export):
The Rust implementation will utilize the arrow crate's FFI module.
code Rust
downloadcontent_copy
expand_less
   use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::Array;

// Function to export data to Java
#[no_mangle]
pub extern "system" fn get_next_batch(
    reader_ptr: *mut c_void, 
    out_array: *mut FFI_ArrowArray, 
    out_schema: *mut FFI_ArrowSchema
) -> i32 {
    let reader = unsafe { &mut *(reader_ptr as *mut RustPartitionReader) };
    
    match reader.fetch_batch() {
        Ok((array_data, schema)) => {
            // "forget" the structs so Rust doesn't drop them when function returns
            // The consumer (Java) is now responsible for calling the release callback
            unsafe {
                std::ptr::write(out_array, FFI_ArrowArray::new(&array_data));
                std::ptr::write(out_schema, FFI_ArrowSchema::try_from(&schema).unwrap());
            }
            0 // Success code
        },
        Err(_) => -1 // Error code
    }
}
 
Java Side (Import):
The Java side uses the Arrow Java libraries to wrap these pointers.
code Java
downloadcontent_copy
expand_less
   public class NativePubSubReader {
    // ... JNI definitions ...

    public boolean next() {
        // Allocate structs on off-heap memory or pass pointers
        long arrayAddr = ArrowArray.allocate(); 
        long schemaAddr = ArrowSchema.allocate();
        
        int status = get_next_batch(nativePtr, arrayAddr, schemaAddr);
        
        if (status == 0) {
            // Zero-copy import
            ArrowArray arrowArray = ArrowArray.wrap(arrayAddr);
            ArrowSchema arrowSchema = ArrowSchema.wrap(schemaAddr);
            
            // This creates the VectorSchemaRoot managing the off-heap memory
            // When this root is closed, it calls the release callback in Rust
            this.currentBatch = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null);
            return true;
        }
        return false;
    }
}
 
Implication: This mechanism ensures that the massive binary blobs of message data are never deserialized into Java byte[] arrays, bypassing the JVM heap entirely.
5.2 Source Architecture: The MicroBatchStream
The MicroBatchStream is the conductor.
planInputPartitions: Returns N instances of PubSubInputPartition. N should be configurable via .option("parallelism", "8").
createReaderFactory: Serializes configuration (Subscription ID, Credentials path) to be sent to Executors.
createPartitionReader: On the Executor, this initializes the JNI bridge.
Authentication: The Rust client must authenticate. The Service Account Key JSON (passed as a string or file path) is used to instantiate the GoogleCloudPubSub client.


Offset Implementation:
Since Pub/Sub messages are unordered and non-sequential, the "Offset" is a checkpoint of the state of the message buffer.
The connector will likely implement a "Commit-Log" approach: The offsets generated are essentially batch IDs. The connector relies on Pub/Sub's own redelivery mechanism for fault tolerance. If a batch isn't committed, the messages aren't Acked.


5.3 Sink Architecture: The AsyncBatchWrite
For writing data to Pub/Sub:
DataWriter: Each task on an executor instantiates a writer.
Write Implementation:
As rows arrive in write(Row row), they are converted to Rust structures.
Optimization: Passing individual rows via JNI is expensive (boundary crossing overhead).
Solution: The Java DataWriter should buffer rows into a small Arrow VectorSchemaRoot (e.g., 1000 rows). Once full, the entire Arrow batch is passed to Rust via the C Data Interface (reverse of the read path).


Rust Publisher:
Receives an Arrow Batch.
Iterates through the batch efficiently.
Uses google-cloud-pubsub's async publisher which handles batching and compression transparently.
Returns a Future/Promise handle to Java.


Commit: The commit() method in Java waits for all Rust-side futures to complete (messages acked by server) before signaling success to Spark.

6. Scenario Handling and Resilience
6.1 Late Arrivals
In Streaming Analytics, "late" is defined relative to the Event Time.
Mechanism: The Rust reader extracts publish_time (native Pub/Sub timestamp) or a user-specified event timestamp column.
Watermark Propagation: Spark requires the source to report the "max event time" seen. The Rust layer must track the maximum timestamp observed in the current buffered batch and return it to the Java layer.
Spark Integration: The Java PartitionReader exposes this via getCurrentWatermark.
Result: Spark's engine uses this to advance the global watermark. Data arriving later than this (minus the threshold) in subsequent batches will be dropped by Spark's stateful operators (e.g., window aggregations), not by the connector.
6.2 Failure Scenarios
Scenario 1: Executor Crash (OOM/Segfault)
Event: An executor process terminates unexpectedly.
Impact: The Rust runtime inside dies. Unacknowledged messages held in the Rust buffer are lost from the perspective of the application.
Recovery: These messages were never Acked to Pub/Sub. The AckDeadline (default 10s or 60s) expires on the Pub/Sub server. Pub/Sub redelivers these messages.
Spark Reaction: Spark detects the task failure. It reschedules the task on a new executor. The new task starts a fresh pull. It receives the redelivered messages.
Data Integrity: Because the previous attempt did not commit, the transaction is clean. Duplicate processing is possible if the crash happened after processing but before Ack. Spark's dropDuplicates or idempotent sinks are required for Exactly-Once consistency.
Scenario 2: Driver Failure
Event: The Spark Driver crashes.
Impact: The Streaming Query stops. No commits happen.
Recovery: Upon restart, Spark reads the Checkpoint Location. It sees the last committed Batch ID. It starts a new Batch.
Pub/Sub State: Messages pulled by the previous (failed) run were likely never Acked (since Ack happens on Commit). They will be redelivered.
Scenario 3: Pub/Sub Outage / Rate Limiting
Event: Pub/Sub API returns UNAVAILABLE or RESOURCE_EXHAUSTED.
Handling: The Rust tonic client must be configured with a robust retry policy (Exponential Backoff).
Backpressure: If the Source cannot pull data, the Spark micro-batch takes longer. Spark's adaptive planning will delay the next trigger. If the Sink cannot publish, the Rust internal buffer fills up. The JNI boundary must propagate this backpressure—if Rust buffer is full, the JNI call to write should block or return a "busy" signal, causing the Spark task to block naturally.
6.3 Autoscaling of Spark Executors
The Challenge:
Standard Spark autoscaling (Dynamic Allocation) often relies on metrics like "backlog" or CPU usage.
Scaling Up: If the processing is slow, Spark requests more executors.
Rebalancing: When a new Executor joins, it starts a new PartitionReader. This opens a new StreamingPull connection.
Pub/Sub Role: Google Pub/Sub supports "Sticky Routing" but generally balances messages across all open connections. The new connection will automatically start receiving a share of the message load. No manual partition rebalancing (like in Kafka) is needed by the connector code itself. The "Virtual Partitioning" strategy (REQ-SRC-02) is key here—having more Spark partitions than current executors allows the scheduler to distribute tasks to new nodes immediately.
Metric for Autoscaling:
Using CPU metrics might be misleading if the bottleneck is I/O.
Best Practice: Use the pubsub.googleapis.com/subscription/num_unacked_messages metric from Stackdriver (Google Cloud Monitoring).
Implementation: The connector can optionally query this metric in the Driver and report it as a custom Spark Metric (backlogSize). The Horizontal Pod Autoscaler (HPA) in Kubernetes can listen to this External Metric to scale the Spark cluster.

7. Testing Strategy
Ensuring robustness requires a multi-layered testing approach.
7.1 Unit Testing
Rust: Use mockall to mock the gRPC client. Verify that:
ArrowArray construction is correct for all data types (Binary, Timestamp, Integers).
Memory is correctly freed when the release callback is invoked.
Panic handling: Ensure Rust panics are caught (std::panic::catch_unwind) and translated into Java Exceptions via JNI, preventing the entire JVM from crashing.


Java: Use JUnit with Mockito. Verify that VectorSchemaRoot correctly wraps the pointers provided by a mock JNI library.
7.2 Integration Testing (The Emulator)
Testing against live cloud is slow. The Google Cloud Pub/Sub Emulator is mandatory.
Test Configuration:
Docker container running gcr.io/google.com/cloudsdktool/cloud-sdk:latest with gcloud beta emulators pubsub start.
Spark Test Suite sets PUBSUB_EMULATOR_HOST environment variable.
Key Integration Tests:
The "Pass-Through" Test: Publish 1000 known integers. Read them in Spark. Verify count == 1000 and sum matches.
The "Hard Stop" Test: Start a stream. Force-kill the generic Spark Worker process. Verify that on restart, the stream resumes and total record count is correct (Exactly-Once verification).
The "Watermark" Test: Publish messages with timestamps T, T-10m, T-1h. Configure watermark 30m. Verify that the T-1h records are dropped by the aggregation query.
7.3 Performance & Stress Testing
Goal: Benchmark throughput vs the standard Google-provided Java connector (spark-pubsub-connector).
Environment: Dataproc cluster, n2-standard-8 nodes.
Workload: 1KB messages, 100,000 msg/sec ingress.
Metrics:
Throughput (Rows/Second).
GC Time (Total GC time / Total Uptime).
Heap Usage (Peak).


Expectation: The Rust connector should show significantly lower Heap Usage and GC Time, allowing for higher consistent throughput.

8. Configuration Reference Table
The following options are defined for the connector:
Parameter
Required
Default
Description
subscriptionId
Yes
-
Full path: projects/X/subscriptions/Y
projectId
Yes
-
GCP Project ID
credentials
No
ADC
Path to Service Account JSON key file.
parallelism
No
spark.default.parallelism
Number of parallel pullers (tasks) to spawn.
batchSize
No
1000
Number of rows per Arrow batch passed to Java.
maxWaitTimeMs
No
100
Max time to wait in Rust to fill a batch before returning partial.
emulatorHost
No
-
host:port for Pub/Sub Emulator (testing).
ackOnCommit
No
true
If true, messages are acked only after Spark Commit.
schema
No
-
DDL string for parsing payload (e.g., JSON schema).


9. Use Case Capabilities
9.1 High-Volume Log Ingest (Cybersecurity)
Scenario: Ingesting 10TB/day of VPC Flow Logs for threat detection.
Requirement: Parsing these logs in Java is expensive.
Solution: The Rust connector parses the raw log line into Arrow columns (Source IP, Dest IP, Port) before passing to Spark. The JVM receives pre-parsed, columnar data, allowing for ultra-fast filtering and aggregation.
9.2 Real-Time IoT Analytics
Scenario: Millions of sensors sending temperature data via MQTT -> Pub/Sub.
Requirement: Handling out-of-order data due to network lag.
Solution: The connector's rigorous Event Time extraction allows Spark's watermark engine to correctly handle late data, updating running aggregates without manual deduplication logic.
9.3 Cloud-to-Cloud Replication
Scenario: Replicating Pub/Sub data to S3 (Delta Lake).
Requirement: Exactly-Once delivery.
Solution: The offset/commit protocol integration ensures that data is written to Delta Lake and checkpointed before the Ack is sent to Pub/Sub. If the write fails, the Ack is withheld, ensuring no data loss.

10. Conclusion and Development Roadmap
Developing a Rust-native Spark connector is a high-complexity, high-reward endeavor. It moves the complexity of memory management from the runtime (GC) to the developer (Rust/JNI), but yields a connector capable of saturating network bandwidth with minimal compute resources.
Phase 1: Foundation (Weeks 1-4)
Set up Rust project with arrow, tonic, tokio.
Implement basic gRPC StreamingPull.
Implement JNI bridge for ArrowArray export.
Unit tests in Rust.
Phase 2: Spark Integration (Weeks 5-8)
Implement DataSourceV2 Source API (MicroBatchStream).
Wire up JNI calls in PartitionReader.
Implement the "Reservoir" offset strategy.
Integration tests with Emulator.
Phase 3: Robustness & Sink (Weeks 9-12)
Implement SupportsWrite Sink API.
Implement Backpressure and Retry logic.
Memory leak profiling (valgrind / jemalloc profiling).
Phase 4: Release & Benchmark (Weeks 13-14)
Package as a JAR (bundling native .so files for linux-x86_64 and linux-aarch64).
Run comparative benchmarks against existing connectors.
Publish documentation.
This PRD provides the necessary blueprint to execute this roadmap, ensuring all critical technical risks are addressed upfront.
Citations
Architecture & Offsets: [cite: 1, 2, 3, 4, 5, 6]
Rust/Arrow/JNI: [cite: 7, 8, 9, 10, 11, 12]
Pub/Sub & Resilience: [cite: 13, 14, 15, 16, 17, 18, 19, 20]
Performance & Scenarios: [cite: 21, 22, 23, 24, 25]
Sources:
apache.org
dataninjago.com
apache.org
databricks.com
dataninjago.com
getorchestra.io
stackoverflow.com
medium.com
stackoverflow.com
apache.org
apache.org
apache.org
medium.com
databricks.com
google.com
databricks.com
github.com
github.com
google.com
google.com
google.com
google.com
google.com
medium.com
medium.com

