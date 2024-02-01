package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonDocCounterMetricsReporter;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonMetricsReporterFactory;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link InsertOneCommand}. */
@ApplicationScoped
public class InsertOneCommandResolver implements CommandResolver<InsertOneCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;
  private final JsonMetricsReporterFactory jsonMetricsReporterFactory;

  @Inject
  public InsertOneCommandResolver(
      Shredder shredder,
      ObjectMapper objectMapper,
      JsonMetricsReporterFactory jsonMetricsReporterFactory) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
    this.jsonMetricsReporterFactory = jsonMetricsReporterFactory;
  }

  @Override
  public Class<InsertOneCommand> getCommandClass() {
    return InsertOneCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, InsertOneCommand command) {
    // Vectorize document
    ctx.tryVectorize(objectMapper.getNodeFactory(), List.of(command.document()));
    JsonDocCounterMetricsReporter jsonDocCounterMetricsReporter =
        jsonMetricsReporterFactory.docJsonCounterMetricsReporter();
    jsonDocCounterMetricsReporter.createDocCounterMetrics(true, ctx.commandName());
    jsonDocCounterMetricsReporter.increaseDocCounterMetrics(1);
    WritableShreddedDocument shreddedDocument =
        shredder.shred(
            command.document(), null, ctx.indexingProjector(), command.getClass().getSimpleName());
    return new InsertOperation(ctx, shreddedDocument);
  }
}
