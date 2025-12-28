Architectural Expansion of Apache Spark Structured Streaming for Google Cloud Pub/Sub: A Deep Analysis of Schema-Aware Ingestion and Native Vectorization
Executive Summary

The modern data engineering landscape is increasingly defined by the tension between the unbounded nature of streaming data and the rigid requirements of enterprise governance. As organizations mature, the "schemaless" freedom of early big data pipelines—often characterized by JSON blobs dumped into data lakes—has proven unsustainable. Data contracts, enforced through rigorous schemas, are now a prerequisite for operational stability. In this context, the integration of Apache Spark Structured Streaming with Google Cloud Pub/Sub represents a critical architectural nexus. While Pub/Sub provides the transport and schema enforcement mechanism via its Schema API (supporting Avro and Protocol Buffers), and Spark provides the computational engine, the connective tissue between them remains underdeveloped in standard libraries.

This report presents a comprehensive architectural strategy to expand Spark’s capabilities to fully support the Google Cloud Pub/Sub ecosystem, specifically targeting the dynamic retrieval and enforcement of Avro and Protocol Buffers (Protobuf) schemas. We argue that the standard "binary blob" approach—where Spark reads bytes and defers parsing to a subsequent map operation—is insufficient for high-integrity pipelines. Instead, we propose a custom DataSource V2 connector that integrates schema resolution directly into the Catalyst optimizer’s planning phase.

Furthermore, acknowledging the computational cost of deserializing complex schemas on the Java Virtual Machine (JVM), this report investigates and proposes a "native" optimization strategy. By leveraging Rust and the Apache Arrow C Data Interface, we outline a mechanism for zero-copy data transfer between the ingestion layer and Spark’s Tungsten execution engine. This hybrid architecture promises to reduce Garbage Collection (GC) pressure and increase throughput by an order of magnitude.

Finally, we address the validation crisis. Testing distributed streaming systems often leads to "boiling the ocean"—spinning up expensive, flaky end-to-end cloud environments for every minor change. We counter this with a tiered testing methodology centered on Property-Based Testing (PBT) for schema fuzzing, local Dockerized emulators for integration stability, and AddressSanitizer (ASan) for native memory safety. This approach ensures robust functionality without the operational overhead of full-scale cloud replication.

1. The Imperative for Schema-Aware Streaming
1.1 The Evolution from Unstructured to Structured Streaming

Apache Spark’s evolution from the RDD-based DStreams to the DataFrame-based Structured Streaming marks a fundamental shift in how distributed systems handle time and data structure. In the DStream era, data was opaque; the engine knew nothing of the payload until execution time. Structured Streaming, introduced in Spark 2.0 and matured through Spark 3.x and 4.x, inverted this relationship [cite: 1, 2]. It treats a stream as an unbounded, append-only table. For this abstraction to hold, the schema must be known—or inferred—before the first byte is processed.

The current limitation in many Pub/Sub integrations is the decoupling of transport and structure. Standard connectors often read Pub/Sub messages as a row containing a timestamp, message ID, and a binary payload [cite: 2, 3]. The developer is then tasked with applying from_avro or from_protobuf functions downstream. This pattern, while flexible, breaks the "data contract" promise at the source. If a producer evolves a schema in a way that breaks compatibility, the Spark job fails at the record level during processing, rather than at the planning level or source level, leading to complex failure recovery scenarios (e.g., dead-letter queue management for schema mismatches).

1.2 The Google Cloud Pub/Sub Schema Paradigm

Google Cloud Pub/Sub has evolved beyond a simple message bus into a governed data transport layer. The introduction of the Schema API allows topics to be strictly typed. A topic can be associated with an Avro schema or a Protobuf definition [cite: 4]. Messages published to such a topic are validated by the Pub/Sub service itself at the ingress point. This server-side validation shifts the responsibility of data quality upstream, preventing "poison pills" from ever reaching the subscription.

However, for a consumer like Spark to leverage this, it must be "schema-aware." It cannot simply assume the data is valid; it must know what the validity rules are. The Schema API allows consumers to retrieve the exact schema revision used to encode a message [cite: 5]. Our expanded architecture requires the Spark connector to dynamically query this API, fetch the definition (JSON for Avro, FileDescriptorSet for Proto), and map it to Spark’s internal StructType representation at runtime. This dynamic linkage ensures that the Spark application evolves in lockstep with the Pub/Sub topic without requiring manual code recompilation for every schema change.

2. Deep Dive: Apache Spark DataSource V2 Architecture

To implement this expanded support, we must operate within the DataSource V2 API. This API was completely rewritten in Spark 2.3/2.4 and further refined in Spark 3.0 to correct the limitations of V1, specifically regarding streaming, columnar reads, and multiple catalog support [cite: 6, 7].

2.1 The TableProvider Interface

The entry point for our expanded connector is the TableProvider (formerly DataSourceV2). This interface is responsible for identifying the data source format (e.g., pubsub-plus) and negotiating the schema.

When a user executes spark.readStream.format("pubsub-plus").load(), the TableProvider is instantiated. Crucially, strictly typed streaming requires the inferSchema method to potentially block and make an external API call to the Google Cloud Schema Registry [cite: 8]. The provider acts as the bridge, fetching the "Active" schema for the target subscription and returning a Spark StructType.

The V2 API separates the concerns of "what to read" (Table capability) from "how to read it" (Scan). The Table instance returned by the provider must implement SupportsRead, signaling to the Catalyst optimizer that this source is capable of generating scan logic [cite: 6, 9].

2.2 The Streaming Scan and MicroBatchStream

In Structured Streaming, the execution model is micro-batch (or continuous, though micro-batch is standard). The Scan interface creates a MicroBatchStream. This is the heartbeat of the connector.

The MicroBatchStream manages offsets. In Kafka, offsets are numerical and contiguous. In Pub/Sub, offsets are opaque acknowledge IDs or publish timestamps. The expanded connector must maintain a stateful mapping of (MessageID, PublishTime) to Spark's Offset abstraction. When planInputPartitions(start, end) is called, the connector calculates the slice of data to read.

This is where the expansion to Avro and Protobuf becomes critical. The PartitionReaderFactory created by the stream needs to be serializable and contain the full schema definition. We cannot rely on the workers having access to the Google Cloud Schema API (due to network restrictions or rate limits). Therefore, the driver must fetch the Avro JSON or Protobuf Descriptor, serialize it, and embed it into the PartitionReaderFactory that is shipped to the executors [cite: 6, 10].

2.3 Columnar vs. Row-Based Reads

Spark’s execution engine, Tungsten, prefers data in a columnar format (Vectors). The standard PartitionReader returns InternalRow objects (row-based). This requires Spark to covert these rows into columns for processing. The V2 API supports a SupportsColumnarReads mixin [cite: 6, 7].

If we implement ColumnarBatch reads, the connector produces vectors directly. This is the foundation for the high-performance "Native" integration discussed in Section 6. By constructing ColumnarBatch objects directly from the raw Pub/Sub bytes (via Arrow), we bypass the row-decoding overhead entirely, feeding Tungsten exactly what it wants [cite: 11, 12].

3. Expanding to Avro: Mechanics and Mapping

Avro is the first citizen of the Pub/Sub schema world. It is widely supported in the Hadoop ecosystem, but rigorous integration requires handling specific edge cases.

3.1 Fetching and Parsing Avro Schemas

The GCP Schema API returns Avro definitions as JSON strings [cite: 13]. The connector must use the org.apache.avro.Schema.Parser to hydrate these strings into Avro Schema objects.
One specific challenge is Schema Evolution. A subscription might contain messages from Schema Revision A and Revision B simultaneously. Spark Structured Streaming generally assumes a fixed schema for the lifetime of the stream (or restart).

Strategy: The connector fetches the latest schema revision at startup.

Backward Compatibility: Avro’s rules allow reading old data with a new schema if fields were added with defaults. The connector must configure the Avro reader with the Writer Schema (from the message attribute) and the Reader Schema (the latest one fetched by the driver).

Integration: We leverage SchemaConverters.toSqlType from the spark-avro module to map the Avro schema to StructType [cite: 14, 15].

3.2 Data Type Fidelity and Complex Unions

Spark’s type system is not isomorphic to Avro.

Unions: Avro supports Union["null", "string", "int"]. Spark SQL does not have a native "Union" type. It typically handles ["null", "T"] as a nullable T. However, complex unions (e.g., ["int", "string"]) are problematic. The expanded connector must map these to a StructType with nullable fields for each option (e.g., { "member0": int, "member1": string }) to preserve information without data loss [cite: 16].

Logical Types: Avro timestamp-millis must be explicitly mapped to Spark TimestampType. If the raw long is read, temporal operations in Spark become cumbersome. The connector’s SchemaConverters logic must be rigorous here to ensure the catalyst optimizer recognizes time columns for windowing operations [cite: 16].

3.3 The GenericRecord Bottleneck

The standard approach uses the Avro library to parse bytes into GenericRecord objects. Spark then uses a RowEncoder to convert GenericRecord to InternalRow [cite: 17, 18].
Implication: This involves double-decoding.

Bytes -> GenericRecord (Heap Object allocation for every field).

GenericRecord -> InternalRow (Copying data to Spark's Unsafe memory).
This "boils the ocean" of memory. For high throughput, we must look to the Native optimization (Section 6).

4. Expanding to Protocol Buffers: The OneOf Challenge

Protobuf integration is significantly more complex due to the lack of a self-describing format and the OneOf construct.

4.1 Dynamic Deserialization without Compilation

Standard Protobuf usage involves compiling .proto files into Java classes (protoc). This contradicts the requirement to "support all schemas" dynamically, as we cannot recompile the connector for every new user topic.
We must use DynamicMessage parsing.

Input: A FileDescriptorSet fetched from the GCP Schema API (which returns base64-encoded descriptors for Protobuf) [cite: 8].

Library: Google’s protobuf-java or the spark-protobuf module (available in newer Spark versions) provides DynamicMessage capabilities. The connector uses DynamicMessage.parseFrom() passing the descriptor [cite: 19, 20].

Mapping: The connector iterates over the FieldDescriptors in the dynamic message to populate the Spark InternalRow.

4.2 The "OneOf" Problem

Protobuf OneOf ensures that only one of a set of fields is set.

code
Protobuf
download
content_copy
expand_less
message Event {
  oneof payload {
    Purchase purchase = 1;
    View view = 2;
  }
}

Spark has no OneOf concept. The expanded connector must map this to a StructType where purchase and view are peer, nullable columns [cite: 21].

Validation: It is impossible for Spark to enforce "exactly one is set" at the schema level. The connector can optionally add a hidden "validation" column or throw a runtime exception if multiple fields are set (though the Protobuf parser itself usually handles this).

Flattening Strategy: For deep analytics, users often prefer OneOfs to be flattened. However, to maintain strict fidelity to the source, keeping them nested in a struct named after the OneOf group is cleaner [cite: 19].

4.3 Handling "Any"

The Protobuf Any type allows embedding arbitrary messages. This is the enemy of structured schemas. The connector acts as a strict gateway: if an Any field is encountered, it can either be returned as a binary blob (opaque) or deserialized into a JSON string if the connector has access to a registry that can resolve the type_url inside the Any [cite: 19]. For the purpose of this report, we recommend mapping Any to BinaryType to avoid infinite recursion dependencies.

5. The Native Frontier: Rust, Arrow, and Zero-Copy

To satisfy the requirement of "not boiling the ocean" regarding resources (CPU/RAM), we propose a high-performance native integration. The JVM overhead for deserializing millions of complex Avro/Proto records per second is substantial. A Rust-based connector leveraging Apache Arrow can eliminate this bottleneck.

5.1 Architecture of the Native Connector

Instead of a Java-only PartitionReader, we implement a Native Vectorized Reader.

JNI Bridge: The Spark Executor calls into a Rust dynamic library (.so/.dll) via JNI.

Rust Worker: The Rust code uses the google-cloud-pubsub crate (async) to pull messages efficiently [cite: 22, 23].

Deserialization:

Avro: Uses the apache-avro Rust crate. This is significantly faster than the Java implementation [cite: 24, 25].

Protobuf: Uses prost and prost-reflect for dynamic message parsing [cite: 20, 26].

Arrow Construction: The critical step. Instead of creating row objects, the Rust code builds Apache Arrow Arrays directly in native memory using arrow-rs [cite: 26, 27].

5.2 The Zero-Copy Handshake (C Data Interface)

The magic lies in passing this data to Spark without copying. Spark (via the spark-arrow module) understands the Arrow C Data Interface [cite: 27, 28].

Mechanism:

Rust populates an ArrowArray struct (a C struct containing pointers to data buffers and validity bitmaps) [cite: 27, 29].

Rust passes the memory address of this struct to Java via JNI (as a long) [cite: 30, 31].

Java uses ArrowArray.wrap(long address) to "adopt" the memory.

Spark wraps this Arrow Vector in a ColumnarBatch and hands it to the Tungsten engine [cite: 11, 12].

This constitutes a Zero-Copy transfer. The actual data (integers, floats, strings) is never copied from Rust heap to Java heap. Java simply views the native memory. This reduces GC pressure to near zero for the data payload.

5.3 Rust Crate Ecosystem for the Connector

The implementation relies on a specific stack of Rust crates:

google-cloud-pubsub: Asynchronous Pub/Sub client. Handles authentication and streaming pulls [cite: 23, 32].

apache-avro: For parsing Avro payloads into Arrow builders [cite: 24, 25].

prost / prost-reflect: For handling Protobuf dynamic messages. prost-reflect is crucial as it allows inspecting .proto descriptors at runtime, mirroring the DynamicMessage capability of Java [cite: 20].

arrow (arrow-rs): The builder API (StructBuilder, UnionBuilder) to construct the vectors [cite: 33, 34].

6. Testing Strategy: Ensuring Reliability Without "Boiling the Ocean"

"Boiling the ocean" in testing refers to relying on massive, slow, expensive, and flaky end-to-end integration tests (e.g., spinning up a real Dataproc cluster and real Pub/Sub topics for every commit). To ensure robust Spark functionality with our expanded connector, we employ a Testing Pyramid strategy.

6.1 Level 1: Property-Based Testing (Schema Fuzzing)

The complexity of Avro/Proto schemas (nesting, unions, recursion) makes example-based testing insufficient. A human developer will rarely think to test a Union["null", "array<map<string, int>>"].
We use Property-Based Testing (PBT) to fuzz the connector’s schema mapping logic.

Tooling: proptest for Rust (native layer) and ScalaCheck for the JVM layer [cite: 35, 36].

Strategy:

Define a Generator that produces random valid Avro Schemas and Protobuf Descriptors (using recursive strategies) [cite: 35].

For each generated schema, generate a random Message payload conforming to it.

Serialize the payload (to binary).

Property: deserialize(serialize(message)) == message.

Feed the binary to the Spark Connector’s deserializer logic.

Assert that the resulting Spark Row / Arrow RecordBatch structurally matches the input.

Outcome: This catches edge cases in OneOf handling, null pointer exceptions in deeply nested structs, and integer overflow issues in logical types, all occurring in milliseconds on a local machine [cite: 37].

6.2 Level 2: The Pub/Sub Emulator (Local Integration)

We eliminate the dependency on real GCP for functional testing using the Google Cloud Pub/Sub Emulator.

Infrastructure: A Docker Compose setup running the official gcloud emulator image [cite: 5].

Configuration: The Spark Connector must respect the PUBSUB_EMULATOR_HOST environment variable. When set, the SchemaServiceClient and Subscriber client must connect to localhost:8085 via plain text (insecure channel), bypassing normal OAuth/GCP credentials [cite: 32].

The Test Suite:

Schema Evolution Test: Programmatically update a topic’s schema in the emulator. Verify Spark streams do not crash and handle the new fields (based on the evolution mode selected) [cite: 5].

Poison Pill Test: Publish a message that does not match the schema. Verify the connector’s error handling (e.g., diverting to a DLQ or counting the metric) without crashing the stream.

Offset Management: Restart the Spark Query. Verify it picks up from the correct message in the Emulator (simulating checkpoint recovery).

6.3 Level 3: Native Memory Sanitization (ASan)

If implementing the Rust/JNI optimization, memory safety is the biggest risk. A segfault in Rust crashes the entire Spark Executor JVM, killing the task.

Tooling: AddressSanitizer (ASan).

Execution:

Compile the Rust library with RUSTFLAGS="-Z sanitizer=address".

Run the Spark local tests with ASAN_OPTIONS=handle_segv=0.

Why handle_segv=0?: The JVM generates generic Segmentation Faults (SIGSEGV) for legitimate purposes (null pointer checks, implicit polling). ASan must be told to ignore these specific signals and focus only on the native heap corruption [cite: 38, 39].

Goal: Detect "Use-After-Free" errors where the Rust side frees the Arrow Array before Java has finished reading it, or where Java tries to access the array after Rust has dropped it.

6.4 Testing Matrix Summary
Test Level	Scope	Tooling	Target	Speed
Unit / Fuzz	Schema Mapping	Proptest, ScalaCheck	SchemaConverters, prost-reflect mappings, UnionBuilder logic	ms
Component	Native/JNI	Rust cargo test, ASan	JNI Boundary, Memory Leaks, Zero-Copy mechanics	sec
Integration	Streaming Logic	Docker, Pub/Sub Emulator	Offsets, Checkpoints, Schema API interaction, Evolution	min
E2E (Staging)	Full System	Real GCP Pub/Sub	Auth, Quotas, Networking (Only run on merge)	hours
7. Data Type Mapping Reference

To ensure the "expand to all" requirement is met, the connector must strictly adhere to the following type mappings.

7.1 Avro to Spark SQL Mapping
Avro Type	Spark SQL Type	Handling Strategy
null	NullType	Handled via nullable fields.
boolean	BooleanType	Direct mapping.
int / long	IntegerType / LongType	Direct mapping.
float / double	FloatType / DoubleType	Direct mapping.
bytes	BinaryType	Direct mapping.
string	StringType	Direct mapping (UTF-8).
record	StructType	Recursive mapping of fields.
enum	StringType	Converted to the name of the symbol.
array	ArrayType	Recursive mapping of items.
map	MapType	Keys are always StringType.
union (Simple)	Nullable Type	["null", "string"] -> StringType (nullable).
union (Complex)	StructType	["int", "string"] -> Struct<member0: int, member1: string>.
fixed	BinaryType	Direct mapping.
timestamp-millis	TimestampType	Logical type conversion required.
7.2 Protobuf to Spark SQL Mapping
Protobuf Type	Spark SQL Type	Handling Strategy
double / float	DoubleType / FloatType	Direct mapping.
int32 / int64	IntegerType / LongType	Direct mapping.
bool	BooleanType	Direct mapping.
string	StringType	Direct mapping.
bytes	BinaryType	Direct mapping.
message	StructType	Recursive mapping.
repeated	ArrayType	Direct mapping.
map	MapType	Direct mapping (Key/Value).
oneof	StructType	A struct containing all oneof fields as nullable. Logic ensures mutual exclusivity (mostly).
Any	BinaryType	Returned as raw bytes or JSON string (if configured).
Timestamp	TimestampType	WKT (Well Known Type) conversion required.
Duration	LongType	Typically mapped to total milliseconds or microseconds.
8. Operational Considerations and Future Outlook
8.1 Schema Caching and Rate Limits

Fetching the schema from the GCP API for every partition is an anti-pattern that leads to throttling (HTTP 429).

Architecture: The Driver fetches the schema once per micro-batch (or caches it with a TTL). The schema definition is then serialized and broadcast to the executors within the task closure. The executors do not contact the Schema API directly.

8.2 Handling Schema Evolution at Runtime

If a stream encounters a message with a schema revision ID that differs from the cached one:

Strict Mode: The task fails. The driver catches the exception, invalidates the cache, re-fetches the new schema, and restarts the query (standard Structured Streaming recovery).

Permissive Mode: If the new schema is backward compatible (e.g., new nullable field), the connector can theoretically adapt. However, Spark's Catalyst engine expects a fixed schema plan. Therefore, a Query Restart is almost always required for schema evolution in Spark [cite: 3]. The connector should provide a customized StreamingQueryListener to automate this restart process.

8.3 Conclusion

The expansion of Spark Structured Streaming to robustly support Google Cloud Pub/Sub with Avro and Protobuf schemas requires a move beyond simple library calls. It demands an architectural shift towards DataSource V2, Dynamic Schema Resolution, and, for high-performance scenarios, Native Vectorized Execution.

By implementing the TableProvider interface to negotiate schemas at runtime, and leveraging the Arrow C Data Interface to bridge the JVM/Rust divide, we achieve a system that is both strictly governed and highly performant. The associated testing strategy—eschewing brittle E2E tests for rigorous Property-Based Testing and Emulators—ensures that this complexity does not become an operational burden. This architecture represents the state-of-the-art for governed streaming ingestion in 2025.

9. Detailed Architectural Components
9.1 Schema Fetcher Implementation (Conceptual)

The SchemaFetcher is a critical component running on the Driver.

code
Scala
download
content_copy
expand_less
object SchemaFetcher {
  // Cache key: Project + SchemaName + RevisionId
  private val schemaCache = new ConcurrentHashMap[String, StructType]()

  def getSchema(projectId: String, schemaId: String, revisionId: String): StructType = {
    val key = s"$projectId:$schemaId:$revisionId"
    schemaCache.computeIfAbsent(key, k => {
      val client = SchemaServiceClient.create()
      val schemaPath = SchemaName.of(projectId, schemaId).toString
      // Fetch specifically the revision
      val schemaObj = client.getSchema(GetSchemaRequest.newBuilder()
         .setName(schemaPath + "@" + revisionId) // Revision syntax
         .setView(SchemaView.FULL)
         .build())
      
      schemaObj.getType match {
        case Schema.Type.AVRO => 
           val avroSchema = new org.apache.avro.Schema.Parser().parse(schemaObj.getDefinition)
           SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
        case Schema.Type.PROTOCOL_BUFFER =>
           val fdSet = FileDescriptorSet.parseFrom(schemaObj.getDefinition.getBytes)
           ProtoConverters.toSqlType(fdSet)
      }
    })
  }
}

Note: This demonstrates the caching logic and the differentiation between Avro and Proto paths [cite: 8, 40].

9.2 Rust JNI Bridge Signature

To support the native reader, the Rust function signature exposed to JNI must look like this:

code
Rust
download
content_copy
expand_less
#[no_mangle]
pub extern "system" fn Java_com_example_spark_pubsub_NativeReader_nextBatch(
    env: JNIEnv,
    _class: JClass,
    reader_ptr: jlong, // Pointer to the Rust Reader struct
    schema_ptr: jlong, // Pointer to C Data Interface ArrowSchema
    array_ptr: jlong   // Pointer to C Data Interface ArrowArray
) -> jboolean {
    // 1. Cast reader_ptr to Rust struct
    // 2. Fetch next batch of Pub/Sub messages
    // 3. Deserialize into Arrow RecordBatch
    // 4. Export RecordBatch to the schema_ptr and array_ptr via FFI
    // 5. Return true if data exists, false if stream finished/timeout
}

This illustrates the raw mechanics required to achieve the Zero-Copy transfer described in Section 5.2 [cite: 27, 30].

9.3 Protobuf "Any" Resolution Logic

When the connector encounters an google.protobuf.Any field:

Extract type_url (e.g., type.googleapis.com/com.example.MyType).

If the connector has a "Known Types" registry (configured at startup), it can attempt to look up the Descriptor for MyType.

If found, it parses the value bytes into a nested Struct.

If not found, it falls back to returning the value as a BinaryType column and the type_url as a generic String column, allowing downstream ETL to handle the resolution. This prevents the connector from crashing on unknown types [cite: 19].

Sources:

medium.com

medium.com

apache.org

google.com

devgenius.io

damavis.com

gitbooks.io

google.com

apache.org

castsoftware.com

github.com

arxiv.org

googleapis.dev

github.com

databricks.com

apache.org

stackoverflow.com

apache.org

databricks.com

docs.rs

badgerbadgerbadgerbadger.dev

github.com

crates.io

docs.rs

lib.rs

crates.io

observablehq.com

github.io

medium.com

stackoverflow.com

stackoverflow.com

docs.rs

apache.org

docs.rs

github.io

ivanyu.me

logrocket.com

kuon.ch

gypsyengineer.com

google.com