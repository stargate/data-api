package io.stargate.sgv2.jsonapi.mcp;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import jakarta.inject.Inject;
import java.util.Map;

public class PoCTools {
  @Inject private RequestContext requestContext;
  @Inject private SchemaCache schemaCache;

  private final CommandContext.BuilderSupplier contextBuilderSupplier;
  private final MeteredCommandProcessor meteredCommandProcessor;

  @Inject
  public PoCTools(
      MeteredCommandProcessor meteredCommandProcessor,
      MeterRegistry meterRegistry,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CqlSessionCacheSupplier sessionCacheSupplier) {
    this.meteredCommandProcessor = meteredCommandProcessor;

    contextBuilderSupplier =
        CommandContext.builderSupplier()
            .withJsonProcessingMetricsReporter(jsonProcessingMetricsReporter)
            .withCqlSessionCache(sessionCacheSupplier.get())
            .withCommandConfig(ConfigPreLoader.getPreLoadOrEmpty())
            .withMeterRegistry(meterRegistry);
  }

  @Tool(description = "Simple Echo tool")
  String echo(@ToolArg(description = "Response", defaultValue = "OK") String response) {
    return response;
  }

  @Tool(description = "System info printer")
  Map<String, String> sysinfo() {
    return Map.of(
        "requestId",
        requestContext.getRequestId(),
        "tenantId",
        requestContext.getTenantId().orElse("N/A"),
        "token",
        requestContext.getCassandraToken().orElse("N/A"));
  }
}
