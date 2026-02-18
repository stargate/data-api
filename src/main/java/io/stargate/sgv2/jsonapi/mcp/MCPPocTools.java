package io.stargate.sgv2.jsonapi.mcp;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import io.quarkiverse.mcp.server.ToolManager;
import io.quarkiverse.mcp.server.ToolResponse;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import jakarta.inject.Inject;
import java.util.Map;

public class MCPPocTools {
  @Inject private ToolManager toolManager;

  @Inject private RequestContext requestContext;
  @Inject private SchemaCache schemaCache;

  private final CommandContext.BuilderSupplier contextBuilderSupplier;
  private final MeteredCommandProcessor meteredCommandProcessor;

  @Inject
  public MCPPocTools(
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
  String echo(
      @ToolArg(description = "Response", defaultValue = "OK", required = false) String response) {
    return response;
  }

  @Tool(description = "System info printer tool")
  Map<String, String> sysinfo() {
    return Map.of(
        "requestId",
        requestContext.getRequestId(),
        "tenantId",
        requestContext.getTenantId().orElse("N/A"),
        "token",
        requestContext.getCassandraToken().orElse("N/A"));
  }

  @Tool(description = "Add Tool tool")
  String addTool(@ToolArg(description = "Name") String name) {
    if (toolManager.getTool(name) != null) {
      return "ALREADY_EXISTS";
    }
    toolManager
        .newTool(name)
        .setDescription("Tool '" + name + "': lower-cases given String")
        .addArgument("value", "Value to convert", true, String.class)
        .setHandler(ta -> ToolResponse.success(ta.args().get("value").toString().toLowerCase()))
        .register();
    return "ADDED";
  }
}
