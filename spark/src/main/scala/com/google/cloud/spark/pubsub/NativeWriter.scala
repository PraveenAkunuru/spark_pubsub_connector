package com.google.cloud.spark.pubsub

class NativeWriter {
  // Ensure library is loaded when instance is created
  NativeLoader.load()

  @native def init(projectId: String, topicId: String): Long
  @native def writeBatch(writerPtr: Long, arrowArrayAddr: Long, arrowSchemaAddr: Long): Int
  @native def close(writerPtr: Long): Unit
}
