package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonBytesMetricsReporter;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link InsertManyCommand}. */
@ApplicationScoped
public class InsertManyCommandResolver implements CommandResolver<InsertManyCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  private final JsonBytesMetricsReporter jsonBytesMetricsReporter;

  @Inject
  public InsertManyCommandResolver(
      Shredder shredder,
      ObjectMapper objectMapper,
      JsonBytesMetricsReporter jsonBytesMetricsReporter) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
    this.jsonBytesMetricsReporter = jsonBytesMetricsReporter;
  }

  @Override
  public Class<InsertManyCommand> getCommandClass() {
    return InsertManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, InsertManyCommand command) {
    // Vectorize documents
    ctx.tryVectorize(objectMapper.getNodeFactory(), command.documents());

    final DocumentProjector projection = ctx.indexingProjector();
    final List<WritableShreddedDocument> shreddedDocuments =
        command.documents().stream()
            .map(
                doc -> shredder.shred(doc, null, projection, command.getClass().getSimpleName()))
            .toList();

    // resolve ordered
    InsertManyCommand.Options options = command.options();

    boolean ordered = null != options && Boolean.TRUE.equals(options.ordered());

    return new InsertOperation(ctx, shreddedDocuments, ordered);
  }
}
