package com.google.cloud.spark.pubsub

object NativeLoader {
  private var loaded = false

  def load(): Unit = synchronized {
    if (!loaded) {
      System.loadLibrary("native_pubsub_connector")
      loaded = true
    }
  }
}
