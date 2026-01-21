package io.stargate.sgv2.jsonapi.config;

import com.google.common.annotations.VisibleForTesting;

/**
 * Static accessor for the debug mode configuration; initialized on startup, used to work around
 * access issues with the Quarkus config system (can access without needing to inject the config
 * interface).
 */
public class DebugConfigAccess {
  private static boolean debugEnabled;

  public static boolean isDebugEnabled() {
    return debugEnabled;
  }

  /**
   * Set the debug mode state. This is called by {@code JsonApiStartUp} during application
   * initialization, but is also used in tests to control the debug mode state.
   *
   * @param state the new debug mode state
   */
  @VisibleForTesting
  public static void setDebugEnabled(boolean state) {
    debugEnabled = state;
  }

  /**
   * Enables debug for the duration of a try-with-resources block.
   * <p/>
   * Example:
   * <pre>
   *   try (DebugScope scope = DebugConfigAccess.withDebugEnabled()) {
   *    // debug is enabled here
   *    } // debug is restored to previous state here
   */
  public static DebugScope withDebugEnabled() {
    return new DebugScope(true);
  }

  public static final class DebugScope implements AutoCloseable {
    private final boolean previous;

    private DebugScope(boolean newValue) {
      previous = isDebugEnabled();
      setDebugEnabled(newValue);
    }

    @Override
    public void close() {
      setDebugEnabled(previous);
    }
  }
}
