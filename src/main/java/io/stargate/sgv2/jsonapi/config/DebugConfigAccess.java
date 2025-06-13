package io.stargate.sgv2.jsonapi.config;

import com.google.common.annotations.VisibleForTesting;

/**
 * Static accessor for the debug mode configuration; initialized on startup, used to work around
 * access issues with the Quarkus config system (can access without needing to inject the config
 * interface).
 */
public class DebugConfigAccess {
  private static boolean debugEnabled;

  public static void initialize(DebugModeConfig config) {
    debugEnabled = config.enabled();
  }

  public static boolean isDebugEnabled() {
    return debugEnabled;
  }

  @VisibleForTesting
  public static void setDebugEnabled(boolean state) {
    debugEnabled = state;
  }
}
