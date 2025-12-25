# Stability and Scale Design Guidelines

To ensure the Spark Pub/Sub connector achieves its high-performance goals while remaining stable at scale, the following design guidelines must be strictly followed.

## 1. Off-Heap Data and Metadata Management
**Goal**: Bypass JVM Garbage Collection (GC) overhead.
- **Vectorized Data Transfer**: Use Apache Arrow C Data Interface to pass pointers of columnar batches from Rust to Spark. The JVM should never manage raw message objects.
- **Native Metadata Reservoirs**: Store `ack_id` strings and other per-message metadata in the Rust-side `PubSubClient` or a dedicated native buffer, *not* in Scala collections.
- **Zero-Copy Acknowledgment**: The `acknowledge` JNI call must operate directly on native memory pointers to avoid string object creation in the JVM.

## 2. Decentralized Acknowledgment (Signal Propagation)
**Goal**: Prevent Driver memory bottlenecks.
- **Driver as Signal Source**: The Driver tracks Batch IDs and issues a "Commit" signal. It *must not* collect `ack_ids` from executors.
- **Executor-Local Acks**: Executors trigger their local native `acknowledge` calls upon receiving a commit signal, leveraging their own network bandwidth.
- **Batch-ID Mapping**: Implement a reservoir pattern where the Driver maps logical Spark offsets to sets of messages held entirely on executors.

## 3. Elastic Virtual Partitioning and Scaling
**Goal**: Handle Spark cluster churn and Pub/Sub connection limits.
- **Over-Partitioning**: Use "Virtual Partitioning" where `numPartitions` > executor cores to allow immediate task distribution during scale-up.
- **Backlog-Aware Scaling**: Report `num_unacked_messages` as a custom Spark metric to Google Cloud Monitoring for autoscaling.
- **Server-Side Balancing**: Rely on Pub/Sub's inherent load balancing across StreamingPull connections.
- **Max Streaming Pulls**: Limit `numPartitions` to a maximum of **2x** the total number of executor cores to prevent connection storms.

## 4. gRPC Connection and Rate Governance
**Goal**: Prevent quota exhaustion and DDoS-like behavior.
- **Quota Awareness**: Respect the `active_streaming_pull_connections` limit (10k/project).
- **Exponential Backoff**: The native tonic client must implement robust exponential backoff for `RESOURCE_EXHAUSTED` errors.
- **Staggered Initialization**: In ultra-high parallelism scenarios, executors must introduce jitter/staggered start for `init` calls to avoid slamming the Pub/Sub API.

## 5. FFI Boundary and Lifecycle Safety
**Goal**: Leak-proof and crash-resistant JVM/Rust interop.
- **Balanced Release Model**: Every memory allocation passed to Java must have a corresponding release callback.
- **Panic Protection**: All native code *must* use `std::panic::catch_unwind` at the JNI boundary to prevent process-level segfaults.
- **Resource Finalization**: Explicitly manage `tokio` runtimes and gRPC channels in `close()` methods.

## 6. Throughput and Latency Optimization
**Goal**: Predictable performance up to 1 GB/s.
- **Asynchronous Sink Publishing**: `DataWriter` buffers rows and triggers async native publish calls.
- **Smart Flush Policies**: Implement "Linger Time" (e.g., 5ms) and Byte-size thresholds (e.g., 5MB) for the write path.
- **Direct Binary Path**: Message payloads must never be deserialized into `byte[]` arrays in the JVM; they stay in off-heap Arrow memory until Catalyst processing.
