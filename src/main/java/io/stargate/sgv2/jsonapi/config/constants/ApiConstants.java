package io.stargate.sgv2.jsonapi.config.constants;

/**
 * This configuration is maintained outside the Quarkus smallrye config system.
 *
 * <p>This is because, the `isOffline` function here is used to decide on how to load the properties
 * from <code> DebugModeConfig</code> via the SmallRye config library in <code>JsonApiException
 * </code> class. So, this property itself can not be loaded from SmallRye config.
 */
public class ApiConstants {
  public static final String OFFLINE_WRITER_MODE_PROPERTY = "stargate.offline.sstablewriter";

  public static boolean isOffline() {
    return Boolean.parseBoolean(System.getProperty(OFFLINE_WRITER_MODE_PROPERTY, "false"));
  }
}
