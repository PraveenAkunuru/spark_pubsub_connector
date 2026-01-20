package finalconnector

import org.scalatest.funsuite.AnyFunSuite

class PubSubPartitionerSuite extends AnyFunSuite {

  test("calculatePartitions respects explicit numPartitions") {
    val result = PubSubPartitioner.calculatePartitions(
      requestedPartitions = Some(5),
      availableCores = 10,
      defaultParallelism = 4,
      expectedMbS = 100
    )
    assert(result == 5)
  }

  test("calculatePartitions calculates intelligent partitioning (Low Throughput)") {
    // Cores = 8. Headroom = 24.
    // 10 MB/s -> 2 partitions floor.
    // Base = Max(2, 24) = 24.
    // HCN(24) = 24.
    val result = PubSubPartitioner.calculatePartitions(
      requestedPartitions = None,
      availableCores = 8,
      defaultParallelism = 4,
      expectedMbS = 10
    )
    assert(result == 24)
  }

  test("calculatePartitions calculates intelligent partitioning (High Throughput)") {
    // Cores = 8. Headroom = 24.
    // 500 MB/s -> 500/8 = 62.5 -> 63.
    // Base = Max(63, 24) = 63.
    // Next HCN after 63 is 120 (1,2,4,6,12,24,36,48,60,120...)
    // Wait, let's check HCN list in logic:
    // 1, 2, 4, 6, 12, 24, 36, 48, 60, 120...
    // Yes.
    
    val result = PubSubPartitioner.calculatePartitions(
      requestedPartitions = None,
      availableCores = 8,
      defaultParallelism = 4,
      expectedMbS = 500
    )
    assert(result == 120)
  }

  test("findNextHighlyCompositeNumber returns exact match") {
    assert(PubSubPartitioner.findNextHighlyCompositeNumber(24) == 24)
    assert(PubSubPartitioner.findNextHighlyCompositeNumber(60) == 60)
  }

  test("findNextHighlyCompositeNumber rounds up") {
    assert(PubSubPartitioner.findNextHighlyCompositeNumber(25) == 36)
    assert(PubSubPartitioner.findNextHighlyCompositeNumber(50) == 60)
  }
}
