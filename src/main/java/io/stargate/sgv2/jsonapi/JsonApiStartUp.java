package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonApiStartUp {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonApiStartUp.class);

  // TODO: why is the DebugModeConfig here ? it is never used
  private final DebugModeConfig config;
  private final OperationsConfig operationsConfig;

  @Inject
  public JsonApiStartUp(DebugModeConfig config, OperationsConfig operationsConfig) {
    this.config = config;
    this.operationsConfig = operationsConfig;
  }

  void onStart(@Observes StartupEvent ev) {
    TenantFactory.initialize(operationsConfig.databaseConfig().type());
  }
}
