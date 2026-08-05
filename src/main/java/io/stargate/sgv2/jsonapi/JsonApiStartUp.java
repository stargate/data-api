package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.StartupEvent;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.DeploymentMarkerConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonApiStartUp {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonApiStartUp.class);

  private final OperationsConfig operationsConfig;
  private final DeploymentMarkerConfig deploymentMarkerConfig;

  /**
   * {@link DebugModeConfig} and {@link BillingConfig} are injected here purely to anchor them as
   * CDI beans, even though we never read them through this constructor — they are consumed
   * elsewhere via {@link io.stargate.sgv2.jsonapi.api.model.command.CommandConfig#get(Class)}.
   *
   * <p>Quarkus ARC removes unused beans at build time, which also drops the SmallRye mapping
   * registration for any {@code @ConfigMapping} interface that has no injection point. When that
   * happens, {@link io.stargate.sgv2.jsonapi.ConfigPreLoader#onStart} fails at startup with {@code
   * SRCFG00027: Could not find a mapping for ...} and every {@code @QuarkusTest} blows up with
   * "Failed to start quarkus". Listing the config interface as a constructor parameter is the
   * minimal way to pin it.
   */
  @Inject
  public JsonApiStartUp(
      DebugModeConfig config,
      OperationsConfig operationsConfig,
      BillingConfig billingConfig,
      DeploymentMarkerConfig deploymentMarkerConfig) {
    this.operationsConfig = operationsConfig;
    this.deploymentMarkerConfig = deploymentMarkerConfig;
  }

  void onStart(@Observes StartupEvent ev) {
    TenantFactory.initialize(operationsConfig.databaseConfig().type());

    // Observation point for deployment drills, see DeploymentMarkerConfig.
    // Disabled by default, in which case nothing is logged here.
    if (deploymentMarkerConfig.enabled()) {
      LOGGER.info(
          "Deployment marker enabled - marker={}", deploymentMarkerConfig.value().orElse(""));
    }
  }
}
