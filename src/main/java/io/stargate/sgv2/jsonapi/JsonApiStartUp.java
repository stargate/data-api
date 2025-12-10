package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.DebugConfigAccess;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

public class JsonApiStartUp {

  private final OperationsConfig operationsConfig;

  @Inject
  public JsonApiStartUp(DebugModeConfig config, OperationsConfig operationsConfig) {
    DebugConfigAccess.setDebugEnabled(config.enabled());
    this.operationsConfig = operationsConfig;
  }

  void onStart(@Observes StartupEvent ev) {
    TenantFactory.initialize(operationsConfig.databaseConfig().type());
  }
}
