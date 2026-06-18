package io.stargate.sgv2.jsonapi.config.constants;

/**
 * This class is used for determining "offline-mode" use of Data API (as a library) it does not use
 * the Quarkus smallrye config system for detection, but is used by {@code CommandConfig} to
 * determine need for alternate loading of Quarkus Smallrye configs.
 */
public class OfflineModeSupport {
  public static final String OFFLINE_WRITER_MODE_PROPERTY = "stargate.offline.sstablewriter";

  public static boolean isOffline() {
    return Boolean.parseBoolean(System.getProperty(OFFLINE_WRITER_MODE_PROPERTY, "false"));
  }
}
