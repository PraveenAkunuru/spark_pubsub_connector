Architectural Design and Implementation Guide: Apache Spark 3.5 Native Connector for Google Cloud Pub/Sub
1. Architectural Foundations and Design Philosophy
The integration of high-velocity streaming data into distributed processing engines presents a fundamental challenge: the impedance mismatch between the continuous, push-based nature of message buses and the micro-batch, pull-based execution model of engines like Apache Spark. When targeting throughputs in excess of 1 GB/s (gigabyte per second) per connector instance, the overhead introduced by the Java Virtual Machine (JVM)—specifically regarding object serialization, garbage collection (GC) pauses, and thread scheduling—becomes the primary bottleneck.
This report details the architectural design for a native Rust-based connector for Apache Spark 3.5 targeting Google Cloud Pub/Sub. The design premise rests on the "Sidecar Pattern" implemented within the Spark Executor process. By offloading the network I/O, protocol negotiation, lease management, and buffer management to a native Rust runtime, we effectively decouple the message ingestion lifecycle from the Spark task execution lifecycle. This allows the connector to maintain active gRPC streaming connections and extend acknowledgement deadlines even during Spark's "Stop-the-World" GC events, preventing the "lease expiration spiral" common in pure JVM implementations.
1.1 The High-Performance Mandate: 1 GB/s Throughput
Achieving 1 GB/s throughput requires a rigorous elimination of memory copies and context switches. A standard implementation using the Google Cloud Java Client Library typically saturates at 200-300 MB/s per core due to the overhead of converting gRPC ByteString objects into Java byte[] arrays, and then into Spark Row objects.
To bypass this limit, the proposed architecture employs the Apache Arrow C Data Interface. This interface allows the Rust runtime to allocate memory buffers aligned to 64-byte boundaries (compatible with AVX-512 SIMD instructions) and pass ownership of these pointers directly to the JVM. Spark's vectorized execution engine (Project Tungsten) can then read directly from these off-heap memory regions without copying the data into the Java Heap. This "Zero-Copy" architecture is the cornerstone of meeting the throughput requirement.
1.2 Leveraging the google-cloud-pubsub Crate
The design strictly adheres to the requirement of utilizing the google-cloud-pubsub Rust crate. This decision is strategic rather than merely convenient. Reimplementing the complexities of the Pub/Sub protocol—specifically StreamingPull, flow control, and automatic lease extension—is error-prone and redundant.
The google-cloud-pubsub crate provides sophisticated internal mechanisms that are critical for stability:
Automatic Lease Extension: The crate spawns background tasks that monitor unacknowledged messages and periodically send modAckDeadline requests to the Pub/Sub server [cite: 1, 2]. This ensures that messages legally "checked out" by the connector are not redelivered to other subscribers while Spark processes a large micro-batch.
Flow Control: The crate implements client-side flow control, limiting the number of outstanding bytes or messages. This backpressure mechanism is essential for preventing Out-Of-Memory (OOM) errors in the Spark Executor when the ingestion rate exceeds the processing rate [cite: 3].
Batching: For the write path, the crate's BatchingConfig allows for the intelligent aggregation of small messages into larger PubsubMessage payloads, optimizing the ratio of payload to gRPC header overhead [cite: 4].
1.3 System Component Overview
The architecture consists of three distinct layers operating within the Spark cluster. The interaction between these layers is governed by the Spark DataSource V2 API and the Java Native Interface (JNI).
Component Layer	Technology Stack	Responsibility
Control Plane	Java / Scala (Spark 3.5)	Query planning, schema inference, task scheduling, offset checkpointing, and transaction boundaries.
Bridge Layer	JNI + Apache Arrow	Exchanging raw memory pointers (FFI_ArrowArray), configuration passing, and signal propagation (e.g., "Commit Batch").
Data Plane	Rust (Tokio + Tonic)	High-performance gRPC I/O, Protocol Buffer parsing, memory allocation, lease management, and background acknowledgement.
The following sections will deconstruct each component, detailing the implementation strategy required to achieve the target performance and reliability guarantees.
2. The Native Bridge: Interoperability and Memory Model
The bridge between the Spark Executor (JVM) and the Rust Sidecar is the critical path for data velocity. A naive JNI implementation involving field-by-field access to Java objects will introduce significant overhead. The design utilizes a shared library architecture where the Rust code is compiled into a .so (Linux) or .dylib (macOS) and loaded by the Executor at runtime.
2.1 The Arrow C Data Interface Strategy
The Arrow C Data Interface provides a stable ABI (Application Binary Interface) for exchanging columnar data structures between languages without serialization [cite: 5, 6]. The Rust implementation utilizes the arrow crate's FFI (Foreign Function Interface) modules to export data.
The core mechanism involves two C-compatible structs: ArrowArray and ArrowSchema.
Allocation in Rust: The Rust PartitionReader accumulates data from Pub/Sub into native Arrow vectors (e.g., BinaryArray for payloads, MapArray for attributes).
Exporting via FFI: Once a batch is ready, Rust calls ArrowArray::into_raw to convert the vector into an FFI_ArrowArray C-struct. This struct contains pointers to the underlying data buffers, validity bitmaps, and offset buffers [cite: 7, 8].
Importing in Java: The Java side allocates an empty ArrowArray structure off-heap. It passes the memory address of this structure to the Rust JNI function. Rust writes the pointers into this address. Java then wraps this address using ArrowArray.wrap(long address) [cite: 9, 10].
Spark Integration: The ArrowRecordBatch obtained in Java is converted into a Spark ColumnarBatch. This batch is passed up the iterator chain to the Catalyst engine.
This approach ensures that the bulk of the data—the message payloads—is never touched by the CPU during the transfer across the language boundary. It resides in memory allocated by Rust's allocator (configured to use jemalloc for fragmentation avoidance) and is read by Spark's UnsafeRow readers.
2.2 Threading Model and the Tokio Runtime
A critical design consideration is the interaction between Spark's thread pool and Rust's async runtime. Spark Executors are multi-threaded, processing multiple Tasks (partitions) concurrently. Rust's google-cloud-pubsub crate relies on tokio for asynchronous I/O.
The Global Runtime Pattern:
It is inefficient to start a new Tokio runtime for every Spark Task. Instead, the design employs a Global Singleton Runtime initialized via the ExecutorPlugin mechanism [cite: 11].
Initialization: When the Executor starts, ExecutorPlugin.init() is called. This loads the Rust native library. The Rust library's initialization routine (JNI_OnLoad or a specific init function) creates a static lazy_static Tokio Runtime [cite: 12].
Task Isolation: Each Spark Task identifies itself via a unique partition ID. When a Task calls into Rust, it obtains a handle to the global runtime to spawn its specific subscription logic.
Context Switching: JNI calls are synchronous. To bridge the sync JNI world with the async Rust world, the connector uses block_on for control operations (connect/disconnect) but relies on shared memory channels (e.g., tokio::sync::mpsc) for data transfer. The actual gRPC polling happens on the Tokio worker threads, independent of the Spark Task thread.
2.3 Managing Off-Heap Memory
To support 1 GB/s, memory turnover is immense. If using a 1 GB batch size, the system allocates and deallocates 1 GB every second.
Allocator: The Rust dynamic library must be linked with jemalloc. The standard system allocator (malloc) often exhibits fragmentation and lock contention under high-threaded workloads typical of Spark Executors.
Buffer Reuse: To further optimize, the Rust implementation should implement buffer pooling. Instead of releasing the Arrow buffers after every batch, the Java side (via a custom ReferenceManager) can release the buffers back to a "Free List" in Rust, allowing the next batch to overwrite the existing memory pages rather than requesting new pages from the OS.
3. Source Connector Design (Read Path)
The Read Path (Source) is responsible for ingesting data from Pub/Sub and exposing it as a structured stream. Spark 3.5 uses the MicroBatchStream interface from DataSource V2.
3.1 Logical Partitioning Strategy
Google Cloud Pub/Sub does not have physical partitions in the same way Kafka does. It uses a global load balancer to route messages to available subscribers. This necessitates a "Partition-per-Executor" strategy.
Partition Planning: The planInputPartitions method in the Driver calculates the number of partitions based on the cluster configuration (e.g., spark.executor.instances * spark.executor.cores).
Dynamic Load Balancing: Each Spark Partition corresponds to an independent StreamingPull connection opened by the Rust sidecar. We rely on Pub/Sub's server-side load balancing to distribute the message load evenly across these connections [cite: 13, 14]. This simplifies the connector design as it removes the need for complex partition assignment logic.
3.2 The Rust Subscriber Implementation
The Rust component manages the Subscriber client. Crucially, the Subscriber must persist across micro-batches. Creating a new Subscriber for every micro-batch (every few seconds) would reset the internal flow control window and ack-deadline heuristics, degrading performance.
Implementation Logic:
Registry: The Rust sidecar maintains a DashMap<String, SubscriberHandle> where the key is a unique identifier generated by the Spark Driver (e.g., subscription_id + partition_id).
StreamingPull: Upon the first request, Rust initializes the google-cloud-pubsub client. It creates a subscription(id) and calls subscribe.
Message Buffer: The callback provided to subscribe pushes ReceivedMessage objects into an unbounded (or large bounded) mpsc channel.
Flow Control Configuration: To hit 1 GB/s, the FlowControlSettings must be tuned.
max_messages: Set high (e.g., 10,000) to ensure the pipeline is full.
max_bytes: Set to a safe fraction of the Executor memory (e.g., 512 MB).
Behavior: The Rust client will automatically stop pulling if the buffer is full, providing backpressure to the server [cite: 3, 15].
3.3 Lease Management and "The Ack Gap"
A critical challenge in Spark-Pub/Sub integration is the "Ack Gap"—the time between reading the data and committing the offset.
The Mechanism: Spark reads a batch. It processes it (transformations, aggregations). It commits the batch. Only then should the messages be acknowledged to Pub/Sub to ensure At-Least-Once semantics.
The Risk: If the processing takes longer than the ack_deadline (default 10s), Pub/Sub will redeliver the messages, causing duplicates.
The Solution: The google-cloud-pubsub crate handles this automatically. As long as the ReceivedMessage struct is held in memory (in the Rust pending_acks map) and not explicitly dropped, the crate's background task will send modAckDeadline extensions [cite: 1, 16]. This is superior to Java connectors where a long GC pause could freeze the lease extension thread. In this design, the Rust background thread runs independent of the JVM heap and GC.
3.4 Offset Management with "AckId" Mapping
Since Pub/Sub has no sequential offsets, the connector must manufacture synthetic offsets for Spark's internal tracking.
Batch ID: The Reader generates a monotonic BatchId.
Mapping: In Rust, a map HashMap<BatchId, Vec<String>> stores the association between a Spark Batch and the list of Pub/Sub AckId strings associated with the messages in that batch [cite: 17].
Commit: When Spark calls commit(BatchId), it passes this ID to Rust. Rust looks up the AckIds and calls .ack() on the stored ReceivedMessage handles.
4. Sink Connector Design (Write Path)
The Write Path (Sink) focuses on high-throughput publication. The target is to publish 1 GB/s, which requires massive parallelism and efficient batching.
4.1 Batching Optimization via google-cloud-pubsub
Achieving high throughput requires minimizing the number of gRPC calls. The google-cloud-pubsub crate provides a Publisher with built-in batching.
Configuration Strategy (BatchingConfig):
max_message_count: Set to 1000.
max_request_bytes: Set to 9 MB (just under the 10 MB gRPC limit).
max_publish_delay: Set to 10-50ms.
Implication: The crate will hold messages in a local buffer until one of these limits is reached, then flush them in a single Publish RPC [cite: 4]. This dramatically increases throughput compared to sending messages individually.
4.2 Handling Concurrency and Ordering
Parallelism: The Sink implementation uses Spark's WriteBuilder to distribute data across all Executors.
Ordering Keys: If the Spark DataFrame contains an ordering key column, it is mapped to the ordering_key field of the PubsubMessage. The google-cloud-pubsub crate guarantees order for messages with the same key, but this can limit throughput due to head-of-line blocking. For maximum throughput (1 GB/s), ordering keys should be avoided unless strictly necessary [cite: 18, 19].
Async Publishing: The Rust implementation iterates the Arrow input batch and spawns a tokio::spawn task for each message publish. To prevent exhausting memory with millions of pending futures, we use a Semaphore or StreamMap to limit the number of concurrent "in-flight" publish requests before they are accepted by the crate's internal buffer.
4.3 Exactly-Once vs. At-Least-Once
Pub/Sub primarily guarantees At-Least-Once delivery. However, the Sink can contribute to end-to-end Exactly-Once semantics when combined with deduplication.
Idempotency: The connector enables the "Exactly-Once Delivery" feature in Pub/Sub by utilizing the publish result future.
Retry Logic: The google-cloud-pubsub crate includes automatic retries for transient errors (e.g., UNAVAILABLE, DEADLINE_EXCEEDED) [cite: 3, 20]. The Rust Sink must await the result of all futures in a batch before returning "Success" to Spark. If any message fails after retries, the entire Spark Task must fail to ensure consistency.
5. Coordination and State Management: The Spark Plugin Architecture
Standard Spark DataSources have a limitation: there is no direct, synchronous channel for the Driver to communicate with specific Executors outside of task dispatch. This makes "Committing" (telling an Executor to Ack messages) difficult.
To solve this, the architecture implements a Spark Plugin [cite: 11, 21].
5.1 The DriverPlugin
The DriverPlugin runs in the Spark Driver process. It hooks into the query lifecycle.
Registration: When the streaming query reaches a checkpoint/commit point, the MicroBatchStream.commit(offset) method is called.
Broadcast: The source implementation invokes the DriverPlugin to broadcast a "Commit Batch X" message to all connected Executors.
Transport: The plugin uses Spark's internal RPC environment to send this signal efficiently [cite: 22].
5.2 The ExecutorPlugin
The ExecutorPlugin runs in every Executor process.
Lifecycle Management: It is responsible for initializing the Global Rust Runtime when the Executor starts. This ensures the native library is loaded only once.
Message Handling: It implements the receive method to handle the "Commit Batch X" RPC from the driver.
JNI Dispatch: Upon receipt, it calls a native function nativeAckBatch(batchId).
Rust Action: The Rust sidecar retrieves the pending AckIds for that batch and processes the acknowledgements asynchronously.
5.3 Failure Recovery and Consistency
Executor Loss: If an Executor dies, the in-memory pending_acks map is lost. The un-acked messages will eventually time out (lease expires) on the Pub/Sub server and be redelivered to other healthy Executors [cite: 23, 24]. This satisfies At-Least-Once delivery.
Driver Failure: If the Driver fails before committing, the new Driver will restart the query from the last checkpoint. It will request offsets that were never committed. The system relies on the standard Spark recovery mechanism; previously fetched but uncommitted data is simply re-fetched (or redelivered).
6. Schema Handling and Serialization
Handling data schemas efficiently is crucial for performance. Converting strictly typed Spark SQL rows to/from raw bytes requires careful design.
6.1 Native Protobuf Handling
Google Cloud Pub/Sub often transports Protocol Buffer messages.
Dynamic Parsing: The connector leverages the prost-reflect crate in Rust. This allows the connector to load a FileDescriptorSet at runtime (passed via Spark options) and dynamically convert Protobuf messages to Arrow structures without generating Rust code at compile time [cite: 25].
Schema Evolution: The SchemaServiceClient from google-cloud-pubsub is used to fetch the topic's schema definition at startup [cite: 26]. This schema is converted into a Spark StructType.
Conversion Path:
Input: Bytes (from Pub/Sub) -> DynamicMessage (via prost-reflect) -> Arrow Array (via field iteration).
Output: Arrow Array -> DynamicMessage -> Bytes -> PubsubMessage.
6.2 Avro Integration
For Avro-encoded payloads, the design utilizes apache-avro in Rust. Similar to Protobuf, the schema is fetched from the Pub/Sub Schema Registry. Arrow's rich type system maps cleanly to Avro, allowing for high-fidelity conversion including complex nested types and unions [cite: 27, 28].
7. Performance Engineering for 1 GB/s
Achieving the gigabyte-per-second target requires optimizing the "Data Plane" specifically.
7.1 Networking and gRPC Channels
A single gRPC channel typically cannot sustain 1 GB/s due to TCP window limits and HTTP/2 framing overhead.
Channel Pooling: The google-cloud-pubsub client configuration must be tuned to open multiple channels. The ClientConfig allows specifying a custom Channel or connection pool size. We recommend 4-8 concurrent channels per Executor for high-throughput workloads.
Load Balancing: Ensure that the Compute Engine instances running the Executors have "Tier 1" networking enabled (if on GCP) and are located in the same region as the Pub/Sub endpoint to minimize latency-induced throughput drops [cite: 19].
7.2 Memory Allocator Tuning
Standard malloc is ill-suited for the allocation patterns of high-throughput streaming (rapid allocation/deallocation of variously sized buffers).
Jemalloc: The Rust library creates a massive number of temporary Bytes objects. Configuring jemalloc as the global allocator in Rust (#[global_allocator]) significantly reduces fragmentation and CPU time spent in the kernel's memory manager.
Vector Capacity: When building Arrow arrays, pre-allocating the vector capacity (Vec::with_capacity) based on the expected batch size eliminates costly memory reallocations during the fetch loop.
7.3 Profiling and Verification
Async Profiler: Use async-profiler on the JVM to ensure the JNI boundary is not causing thread contention.
Rust Flamegraphs: Use flamegraph on the Rust sidecar to identify CPU hotspots in the Protobuf parsing or Arrow conversion logic.
Metric: The primary metric for success is Ingest Rate (MB/s) measured at the Spark Sink. Secondary metrics include GC Time (should be negligible) and Ack Latency (should remain stable).
8. Implementation Reference
The following sections provide the concrete technical specifications for the implementation.
8.1 Rust Crate Configuration (Cargo.toml)
code
Toml
[package]
name = "spark_pubsub_native"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"] # Essential for loading via System.loadLibrary

[dependencies]
# Async Runtime
tokio = { version = "1.32", features = ["full"] }

# Google Cloud SDK
google-cloud-pubsub = { version = "0.28", features = ["auth"] }
google-cloud-googleapis = "0.14"

# Arrow Ecosystem
arrow = { version = "53.0", features = ["ffi"] }

# Interoperability
jni = "0.21"
prost = "0.13"
prost-types = "0.13"
prost-reflect = "0.16" # For dynamic Protobuf handling

# Utilities
lazy_static = "1.4"
dashmap = "5.5" # For concurrent Subscriber registry
log = "0.4"
env_logger = "0.11"
jemallocator = "0.5" # Critical for performance
8.2 Configuration Parameter Mapping
To expose the google-cloud-pubsub internal capabilities to the Spark user, the connector maps Spark configuration options directly to Rust client settings.
Spark Option (.option())	Rust ClientConfig / BatchingConfig	Description	Default
pubsub.subscription.id	SubscriptionConfig	Target Subscription ID	Required
pubsub.flowcontrol.maxMessages	FlowControlSettings.max_messages	Max un-acked messages per Executor	1000
pubsub.flowcontrol.maxBytes	FlowControlSettings.max_bytes	Max un-acked bytes per Executor	100 MB
pubsub.batching.elementCount	BatchingConfig.max_message_count	Messages per Write Batch	100
pubsub.batching.requestBytes	BatchingConfig.max_request_bytes	Bytes per Write Batch	9 MB
pubsub.batching.delayThreshold	BatchingConfig.max_publish_delay	Max wait time before flush	10 ms
pubsub.writer.threads	Tokio Runtime	Number of Tokio worker threads	4
8.3 Testing Strategy for 1 GB/s
Testing a high-performance connector requires a dedicated environment.
Environment Setup:
Cluster: Google Dataproc or GKE.
Nodes: n2-standard-32 (32 vCPUs, 128 GB RAM).
Network: 100 Gbps Tier 1 networking.
Data Generation:
Do not use a single publisher. Use a distributed load generator (e.g., a separate Spark job) to flood the topic with 1KB messages.
Target Ingest Rate: 1,000,000 messages/second.
Throughput Test:
Configure the Connector with pubsub.flowcontrol.maxMessages = 10000.
Run a Spark Structured Streaming job: readStream.format("pubsub")...writeStream.format("noop").
Measure the "Input Rate" in the Spark UI. It should stabilize at ~1 million rec/sec across the cluster.
Resilience Test:
While the 1 GB/s load is running, terminate a random Executor node.
Verify that the Spark UI shows a momentary dip, followed by recovery as the scheduler reschedules the partition on a new node.
Verify no data loss (at-least-once) by checking the final counts in a deduplicated sink (e.g., BigQuery or Delta Lake).
8.4 Failure Scenarios and Mitigations
Scenario	Consequence	Mitigation
Executor OOM	JVM Crash	Native FlowControl limits bytes pulled. jemalloc handles fragmentation.
Long GC Pause	Heartbeat Miss	Rust Sidecar runs on separate threads; google-cloud-pubsub maintains lease independently.
Driver Failure	Commit Loss	New Driver restarts query. Un-acked messages expire and are redelivered by Pub/Sub.
Network Partition	I/O Error	google-cloud-pubsub has internal retry logic for idempotent failures. Connector surfaces fatal errors to Spark.
9. Conclusion
This design represents a shift from traditional Java-centric Spark connectors. By treating the Spark Executor as a host for a specialized, high-performance Rust runtime, we eliminate the structural inefficiencies that prevent pure-JVM solutions from reaching gigabyte-scale throughput. The combination of Spark Plugins for lifecycle management, Arrow C Data Interface for zero-copy transfer, and the google-cloud-pubsub crate for protocol correctness creates a robust, industry-grade solution capable of handling the most demanding streaming workloads on Google Cloud.
10. Detailed Implementation Logic
10.1 Rust Source Code Structure (source.rs)
The following Rust implementation detail demonstrates the PartitionReader logic, managing the buffer and JNI export.
code
Rust
use arrow::array::{Array, BinaryArray, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use google_cloud_pubsub::subscriber::ReceivedMessage;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

// State maintained for each active partition
pub struct PartitionReaderState {
    // Channel to receive messages from the background Tokio task
    receiver: mpsc::Receiver<ReceivedMessage>,
    // Store ack_ids to handle commit later
    pending_acks: Vec<String>,
}

impl PartitionReaderState {
    // Called by JNI to fetch the next micro-batch
    pub fn next_batch(&mut self, batch_size: usize) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let mut messages = Vec::with_capacity(batch_size);
        
        // Non-blocking drain of the channel buffer
        while messages.len() < batch_size {
            match self.receiver.try_recv() {
                Ok(msg) => messages.push(msg),
                Err(_) => break, // Channel empty
            }
        }

        if messages.is_empty() {
            // Return empty arrays if no data
            return empty_arrow_batch();
        }

        // 1. Extract Payloads (Zero Copy where possible)
        let payloads: Vec<&[u8]> = messages.iter()
            .map(|m| m.message.data.as_slice())
            .collect();

        // 2. Extract Attributes
        // ... (Logic to build MapArray for attributes)

        // 3. Store Ack IDs for this batch
        // In a real impl, these would be keyed by a BatchID passed from Spark
        for msg in &messages {
            self.pending_acks.push(msg.ack_id().to_string());
        }

        // 4. Convert to Arrow BinaryArray
        let binary_array = BinaryArray::from(payloads);
        let arrow_data = binary_array.into_data();

        // 5. Export via C Data Interface
        // This transfers ownership of the pointers to Java
        let (out_array, out_schema) = ArrowArray::into_raw(arrow_data);
        (out_array, out_schema)
    }
}
10.2 Java JNI Wrapper (NativeBindings.java)
The Java side acts as a thin wrapper, orchestrating the calls.
code
Java
public class NativeBindings {
    static {
        System.loadLibrary("spark_pubsub_native");
    }

    // Initializes the Subscriber in Rust and returns a pointer/handle
    public static native long initSubscriber(String subscriptionId, Map<String, String> config);

    // Fills the Arrow structures at the given memory addresses
    public static native int nextBatch(long handle, long arrayAddress, long schemaAddress, int batchSize);

    // Triggers the acknowledgement of a specific batch
    public static native void ackBatch(long handle, long batchId);
    
    // Clean up Rust resources
    public static native void close(long handle);
}
This comprehensive design ensures that every requirement—from leveraging internal crate capabilities to defining a rigorous testing strategy—is met with a focus on maximum throughput and reliability.