package com.google.cloud.spark.pubsub

import java.io.{File, FileOutputStream, InputStream}
import java.nio.file.Files

/**
 * Handles the loading of the native Rust shared library.
 *
 * This loader ensures that the native library is only loaded once per JVM.
 * It follows a two-step loading strategy:
 * 1. **System Property**: Attempts to load via `System.loadLibrary` (using `java.library.path`).
 * 2. **Classpath Extraction**: If step 1 fails, it extracts the bundled `.so` from the JAR
 *    to a temporary directory and loads it via `System.load`.
 */
object NativeLoader {
  private var loaded = false
  private val LIB_NAME = "native_pubsub_connector"

  /**
   * Triggers the loading of the native library if it hasn't been loaded yet.
   */
  def load(): Unit = synchronized {
    if (!loaded) {
      try {
        // Step 1: Try java.library.path
        System.loadLibrary(LIB_NAME)
        loaded = true
      } catch {
        case _: UnsatisfiedLinkError =>
          // Step 2: Fallback to classpath extraction
          loadFromClasspath()
          loaded = true
      }
    }
  }

  private def loadFromClasspath(): Unit = {
    val platformPath = getPlatformPath
    val resourcePath = s"/$platformPath/lib$LIB_NAME.so"
    
    val inputStream = getClass.getResourceAsStream(resourcePath)
    if (inputStream == null) {
      throw new RuntimeException(s"Native library $resourcePath not found in classpath. Supported platforms: linux-x86-64, linux-aarch64, darwin-x86-64, darwin-aarch64.")
    }

    try {
      val tempDir = Files.createTempDirectory("spark_pubsub_native").toFile
      tempDir.deleteOnExit()
      val tempFile = new File(tempDir, s"lib$LIB_NAME.so")
      tempFile.deleteOnExit()

      val outputStream = new FileOutputStream(tempFile)
      try {
        val buffer = new Array[Byte](1024 * 8)
        var bytesRead = 0
        while ({ bytesRead = inputStream.read(buffer); bytesRead != -1 }) {
          outputStream.write(buffer, 0, bytesRead)
        }
      } finally {
        outputStream.close()
      }

      System.load(tempFile.getAbsolutePath)
    } finally {
      inputStream.close()
    }
  }

  private def getPlatformPath: String = {
    val os = System.getProperty("os.name").toLowerCase
    val arch = System.getProperty("os.arch").toLowerCase

    val osName = if (os.contains("linux")) "linux"
    else if (os.contains("mac") || os.contains("darwin")) "darwin"
    else throw new RuntimeException(s"Unsupported OS: $os")

    val archName = if (arch == "amd64" || arch == "x86_64") "x86-64"
    else if (arch == "aarch64" || arch == "arm64") "aarch64"
    else throw new RuntimeException(s"Unsupported Architecture: $arch")

    s"$osName-$archName"
  }
}
