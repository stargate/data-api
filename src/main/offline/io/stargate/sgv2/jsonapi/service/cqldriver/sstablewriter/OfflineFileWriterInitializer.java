package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import io.smallrye.context.SmallRyeContextManagerProvider;
import io.stargate.sgv2.jsonapi.config.constants.ApiConstants;

public class OfflineFileWriterInitializer {

  public static void initialize() {
    System.setProperty(ApiConstants.OFFLINE_WRITER_MODE_PROPERTY, "true");
    SmallRyeContextManagerProvider.instance();
  }
}
