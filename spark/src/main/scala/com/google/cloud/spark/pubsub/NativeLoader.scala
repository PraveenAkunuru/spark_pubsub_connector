package com.google.cloud.spark.pubsub

/**
 * Handles the loading of the native Rust shared library.
 *
 * This loader ensures that the native library is only loaded once per JVM.
 * Currently, it expects the library to be present on the `java.library.path`.
 */
object NativeLoader {
  private var loaded = false

  /**
   * Triggers the loading of the native library if it hasn't been loaded yet.
   */
  def load(): Unit = synchronized {
    if (!loaded) {
      System.loadLibrary("native_pubsub_connector")
      loaded = true
    }
  }
}
