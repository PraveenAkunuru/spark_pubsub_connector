package finalconnector

import org.apache.spark.internal.Logging

object PubSubPartitioner extends Logging {
  
  /**
   * Calculates the target number of partitions based on environment and config.
   * 
   * @param requestedPartitions Option[Int] from configuration
   * @param availableCores Total cores available in the cluster (or default parallelism)
   * @param defaultParallelism Spark default parallelism
   * @param expectedMbS Target throughput in MB/s
   * @return The optimal number of partitions
   */
  def calculatePartitions(
      requestedPartitions: Option[Int],
      availableCores: Int,
      defaultParallelism: Int,
      expectedMbS: Int): Int = {
      
    requestedPartitions.getOrElse {
      // Use the max of config-based cores and current active cores/default
      val cores = Math.max(availableCores, defaultParallelism)
      
      val tFloor = Math.ceil(expectedMbS / 8.0).toInt
      val pHeadroom = cores * 3
      
      val base = Math.max(tFloor, pHeadroom)
      val hcn = findNextHighlyCompositeNumber(base)
      
      logDebug(s"Intelligent Partitioning: cores=$cores, expectedMbS=$expectedMbS => base=$base, hcn=$hcn")
      hcn
    }
  }

  /**
   * Returns the smallest Highly Composite Number greater than or equal to n.
   */
  def findNextHighlyCompositeNumber(n: Int): Int = {
    val hcns = Array(
      1, 2, 4, 6, 12, 24, 36, 48, 60, 120, 180, 240, 360, 720, 840, 1260, 1680, 2520, 5040, 7560, 10080, 15120, 20160, 25200, 27720, 45360, 50400, 55440, 83160, 110880
    )
    hcns.find(_ >= n).getOrElse(n)
  }
}
