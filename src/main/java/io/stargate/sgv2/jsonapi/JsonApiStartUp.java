package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonApiStartUp {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonApiStartUp.class);
  private final DebugModeConfig config;

  @Inject
  public JsonApiStartUp(DebugModeConfig config) {
    this.config = config;
  }

  void onStart(@Observes StartupEvent ev) {}
}
