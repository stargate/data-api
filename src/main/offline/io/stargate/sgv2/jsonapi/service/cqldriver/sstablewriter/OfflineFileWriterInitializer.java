package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.OFFLINE_WRITER;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.context.SmallRyeContextManagerProvider;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.ApiConstants;

public class OfflineFileWriterInitializer {

  public static void initialize() {
    System.setProperty(ApiConstants.OFFLINE_WRITER_MODE_PROPERTY, "true");
    SmallRyeContextManagerProvider.instance();
  }

  public static OperationsConfig buildOperationsConfig() {
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", OFFLINE_WRITER)
            .build();
    return smallRyeConfig.getConfigMapping(OperationsConfig.class);
  }
}
