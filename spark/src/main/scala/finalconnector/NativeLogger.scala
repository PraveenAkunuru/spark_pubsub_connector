package finalconnector

import org.slf4j.LoggerFactory

/**
 * Singleton entry point for Native (Rust) logs to generic Spark Log4j/SLF4J.
 * 
 * This object is called via JNI from the Rust side (`diagnostics/logging.rs`).
 * It ensures that native operational logs are centralized with Spark logs 
 * for consistent debugging and monitoring.
 */
object NativeLogger {
  private val logger = LoggerFactory.getLogger("NativePubSub")

  /**
   * Log a message from Rust with a specific level.
   *
   * @param level Level mapping from Rust's `log` crate:
   *              1 = Error, 2 = Warn, 3 = Info, 4 = Debug, 5 = Trace
   * @param msg   The log message produced by Rust
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
