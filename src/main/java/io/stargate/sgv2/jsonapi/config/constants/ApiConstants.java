package io.stargate.sgv2.jsonapi.config.constants;

public class ApiConstants {
  public static final String OFFLINE_WRITER_MODE_PROPERTY = "stargate.offline.sstablewriter";

  public static boolean isOffline() {
    return Boolean.parseBoolean(System.getProperty(OFFLINE_WRITER_MODE_PROPERTY, "false"));
  }
}
