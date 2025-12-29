package finalconnector

import java.io.{File, FileOutputStream}
import java.nio.file.Files

/**
 * Handles the secure loading and isolation of the native Rust shared library.
 *
 * ## Absolute Isolation Principle
 * Spark runtimes often suffer from "Transitive Debt" where native libraries
 * loaded by one component interfere with another, or "Executor Loading Gap" 
 * where libraries present on the driver are missing on executors.
 *
 * This loader solves these by:
 * 1. **Idempotent Loading**: Ensuring the `.so` is loaded exactly once per JVM.
 * 2. **Fallback Extraction**: If the library isn't in `java.library.path`, it 
 *    automatically extracts the platform-correct binary from the connector JAR.
 * 3. **Dynamic Pathing**: Creating unique temp directories to avoid library shadowing.
 */
object NativeLoader {
  private var loaded = false
  private val LIB_NAME = "native_pubsub_connector"

  /**
   * Triggers the loading of the native library.
   *
   * This method is typically called in the static initializer or class body 
   * of classes that define JNI methods (e.g., NativeReader, NativeWriter).
   */
  def load(): Unit = synchronized {
    if (!loaded) {
      try {
        // Primary Strategy: System path (Standard environment configuration)
        System.loadLibrary(LIB_NAME)
        println(s"NativeLoader: Successfully mapped $LIB_NAME via library path.")
        loaded = true
      } catch {
        case e: UnsatisfiedLinkError =>
          // Secondary Strategy: Internal JAR extraction (Self-contained deployment)
          println(s"NativeLoader: $LIB_NAME not found in system path. Attempting classpath extraction...")
          loadFromClasspath()
          loaded = true
      }
    }
  }

  /**
   * Extracts the native library from the JAR resources to a temporary file and loads it.
   */
  private def loadFromClasspath(): Unit = {
    val platformPath = getPlatformPath
    val extension = if (platformPath.startsWith("darwin")) "dylib" else "so"
    val resourcePath = s"/$platformPath/lib$LIB_NAME.$extension"
    
    val inputStream = getClass.getResourceAsStream(resourcePath)
    if (inputStream == null) {
      throw new RuntimeException(s"Native Deployment Error: $resourcePath not found in JAR. " +
        s"Supported platforms: linux-x86-64, linux-aarch64, darwin-x86-64, darwin-aarch64.")
    }

    try {
      // Create a unique temporary directory to avoid conflicts between multiple Spark applications
      // on the same node.
      val tempDir = Files.createTempDirectory("spark_pubsub_native_").toFile
      tempDir.deleteOnExit()
      val tempFile = new File(tempDir, s"lib$LIB_NAME.$extension")
      tempFile.deleteOnExit()

      val outputStream = new FileOutputStream(tempFile)
      try {
        val buffer = new Array[Byte](1024 * 32) // Larger buffer for faster extraction
        var bytesRead = 0
        while ({ bytesRead = inputStream.read(buffer); bytesRead != -1 }) {
          outputStream.write(buffer, 0, bytesRead)
        }
      } finally {
        outputStream.close()
      }

      System.load(tempFile.getAbsolutePath)
      println(s"NativeLoader: Successfully extracted and loaded $LIB_NAME from $resourcePath")
    } finally {
      inputStream.close()
    }
  }

  /**
   * Detects the OS and architecture to locate the correct native binary.
   */
  private def getPlatformPath: String = {
    val os = System.getProperty("os.name").toLowerCase
    val arch = System.getProperty("os.arch").toLowerCase

    val osName = if (os.contains("linux")) "linux"
    else if (os.contains("mac") || os.contains("darwin")) "darwin"
    else throw new RuntimeException(s"Unsupported Platform OS: $os")

    val archName = if (arch == "amd64" || arch == "x86_64") "x86-64"
    else if (arch == "aarch64" || arch == "arm64") "aarch64"
    else throw new RuntimeException(s"Unsupported Platform Architecture: $arch")

    s"$osName-$archName"
  }
}
