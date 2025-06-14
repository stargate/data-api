package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.config.DebugConfigAccess;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

public class JsonApiStartUp {
  @Inject
  public JsonApiStartUp(DebugModeConfig config) {
    DebugConfigAccess.initialize(config);
  }

  void onStart(@Observes StartupEvent ev) {}
}
