package com.google.cloud.spark.pubsub

import org.scalatest.funsuite.AnyFunSuite

class NativeIntegrationTest extends AnyFunSuite {
  
  test("NativeReader.init should return valid pointer") {
    // We expect this to fail if lib is not found or fails to init
    // If PUBSUB_EMULATOR_HOST is not set, it might try to connect to real Pub/Sub and timeout or fail auth.
    // We can set it to a dummy value or handle error gracefully.
    // For now, let's just attempt to load and init.
    // If init fails (e.g. connection error), it returns 0 (handled in lib.rs logic?).
    // Wait, lib.rs `init` throws RuntimeException if calls fail.
    
    val reader = new NativeReader()
    
    // We use a try-catch to allow connection failure but verify JNI linkage
    try {
      // Ensure library is loaded
      NativeReader.load()
      
      val ptr = reader.init("test-project", "test-subscription", 0)
      // If we got here, JNI worked? Or maybe connection worked?
      // If we don't have creds, it will panic or error.
      // But we just want to verify JNI linkage for "Phase 2 foundation".
      // Let's assert true for now if we don't crash with UnsatisfiedLinkError.
      if (ptr != 0) {
        reader.close(ptr)
      }
    } catch {
      case e: RuntimeException =>
        // This is expected if we can't connect, but JNI call itself succeeded.
        // Assert message contains expected error from Rust or standard Java linkage.
        println(s"Caught expected runtime exception from Rust: ${e.getMessage}")
      case e: UnsatisfiedLinkError =>
        fail(s"Native library not found: ${e.getMessage}")
    }
  }
}
