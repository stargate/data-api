package io.stargate.sgv2.jsonapi.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Provides an {@link AutoCloseable} wrapper around a {@link java.util.logging.Logger} that allows
 * capturing log records for testing purposes. When exiting the try block the log level is restored
 * to its previous value.
 *
 * <p>Example usage:
 *
 * <pre>
 *     try (var logWrapper = new LoggerTestWrapper(SuperShreddingTablePredicate.class)) {
 *        // do testing
 *
 *         assertThat(logWrapper.logMessages())
 *             .anyMatch(s -> s.contains("the message I expect"));
 *     }
 * </pre>
 */
public class LoggerTestWrapper implements AutoCloseable {

  // NOTE: using the java logger packages so we can change the logging level
  private final java.util.logging.Logger targetLogger;
  private final java.util.logging.Level previoiusLevel;
  private final java.util.logging.Handler memoryHandler;

  private static final int MAX_RECORDS = 1000;
  public final Deque<LogRecord> records = new ArrayDeque<>();

  /**
   * Changes the log level for the logger to {@link Level#FINEST}
   *
   * @param clazz Name of the logger to change.
   */
  public LoggerTestWrapper(Class<?> clazz) {
    this(clazz, Level.FINEST);
  }

  /**
   * Changes the log level for the logger to the specified level, while inside the auto closeable
   *
   * @param clazz Name of the logger to change.
   * @param newLevel The new log level.
   */
  public LoggerTestWrapper(Class<?> clazz, java.util.logging.Level newLevel) {

    Objects.requireNonNull(clazz, "clazz cannot be null");
    Objects.requireNonNull(newLevel, "newLevel cannot be null");

    this.targetLogger = java.util.logging.Logger.getLogger(clazz.getName());
    this.previoiusLevel = targetLogger.getLevel();
    targetLogger.setLevel(newLevel);

    this.memoryHandler =
        new java.util.logging.Handler() {
          public void publish(java.util.logging.LogRecord r) {
            if (records.size() >= MAX_RECORDS) {
              records.pollFirst();
            }
            records.addLast(r);
          }

          public void flush() {}

          public void close() {}
        };
    this.memoryHandler.setLevel(newLevel);
    targetLogger.addHandler(memoryHandler);
  }

  public List<LogRecord> logRecords() {
    return List.copyOf(records);
  }

  public List<String> logMessages() {
    return records.stream().map(LogRecord::getMessage).toList();
  }

  @Override
  public void close() {
    targetLogger.setLevel(previoiusLevel);
    targetLogger.removeHandler(memoryHandler);
  }
}
