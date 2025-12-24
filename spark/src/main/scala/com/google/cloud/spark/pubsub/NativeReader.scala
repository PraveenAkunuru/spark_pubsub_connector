package com.google.cloud.spark.pubsub

class NativeReader {
  // Ensure library is loaded when instance is created
  NativeLoader.load()

  @native def init(projectId: String, subscriptionId: String): Long
  @native def getNextBatch(readerPtr: Long, arrowArrayAddr: Long, arrowSchemaAddr: Long): Int
  @native def close(readerPtr: Long): Unit
}

object NativeReader {
  // Force loading
  def load(): Unit = {
    NativeLoader.load()
  }
}
