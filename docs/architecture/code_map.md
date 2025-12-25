# Codebase Map: Spark Pub/Sub Connector

This document providing a high-level overview of all modules in the project and their specific roles.

## 1. Native Layer (Rust)
Located in `native/src/`. Handles high-performance I/O and Arrow conversion.

| Module | Purpose | Role |
| :--- | :--- | :--- |
| `lib.rs` | **JNI Entry Point** | Defines the JNI bridge using `robusta_jni`. Manages native runtime lifecycle and FFI memory handoff. Implements `storeAcksFromArrow` and `ackCommitted`. |
| `pubsub.rs` | **Pub/Sub Client** | Orchestrates background `StreamingPull` tasks and `PublisherClient` gRPC calls. Manages `ACK_RESERVOIR` (off-heap) and `CONNECTION_POOL`. |
| `arrow_convert.rs` | **Arrow Engine** | Handles transformation between Pub/Sub messages and Arrow `RecordBatch` formats. Defines the columnar schema. |

## 2. Spark Layer (Scala)
Located in `spark/src/main/scala/com/google/cloud/spark/pubsub/`. Implements Spark DataSource V2 API.

### Control Plane
| Class | Role |
| :--- | :--- |
| `PubSubTableProvider` | **Bootstrap**: Handles `pubsub-native` registration and initial schema inference. |
| `PubSubTable` | **Logical Table**: Declares read/write capabilities and builds scan/write builders. |
| `PubSubMicroBatchStream` | **Streaming Engine**: Manages offsets and plans parallel partitions for executors. |
| `PubSubConfig` | **Configuration**: Centralized keys for project ID, topic, subscription, and batch sizes. |

### Data Plane (Executors)
| Class | Role |
| :--- | :--- |
| `PubSubPartitionReader` | **Native Ingest**: Initializes `NativeReader`, manages Arrow memory, and notifies Native of committed batches via signal propagation. |
| `PubSubDataWriter` | **Native Egress**: Buffers rows, exports them to Arrow FFI, and calls `NativeWriter` for gRPC publishing. |
| `NativeReader` / `NativeWriter` | **JNI Proxies**: Thin Scala wrappers around `@native` methods. |
| `NativeLoader` | **Library Lifecycle**: Bundles the `.so` in JAR and handles automated extraction/loading for cluster portability. |
| `ArrowUtils` | **Schema & Value Mapping**: Centralized translation between Spark `InternalRow` and Arrow `FieldVector`. |

## 3. Deployment & Build
| File | Role |
| :--- | :--- |
| `native/Cargo.toml` | Manages Rust dependencies (`tonic`, `arrow`, `robusta_jni`). |
| `spark/build.sbt` | Multi-module build for Spark 3.3, 3.5, and 4.0 compatibility. |
| `run_ack_test.sh` | Automated integration test runner including Emulator setup. |
| `AckIntegrationTest.scala` | End-to-end verification of At-Least-Once (Ack-on-Commit) delivery. |
