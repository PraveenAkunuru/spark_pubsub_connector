package com.google.cloud.spark.pubsub.diagnostics

import org.slf4j.LoggerFactory

/**
 * Singleton entry point for Native (Rust) logs to generic Spark Log4j.
 * Called via JNI from logging.rs.
 */
object NativeLogger {
  private val logger = LoggerFactory.getLogger("NativePubSub")

  /**
   * Log a message from Rust with a specific level.
   * Levels match Rust's log crate:
   * 1 = Error
   * 2 = Warn
   * 3 = Info
   * 4 = Debug
   * 5 = Trace
   */
  def log(level: Int, msg: String): Unit = {
    level match {
      case 1 => logger.error(msg)
      case 2 => logger.warn(msg)
      case 3 => logger.info(msg)
      case 4 => logger.debug(msg)
      case 5 => logger.trace(msg)
      case _ => logger.info(s"[UNKNOWN LEVEL $level] $msg")
    }
  }
}
