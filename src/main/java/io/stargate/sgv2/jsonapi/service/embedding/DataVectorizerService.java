package io.stargate.sgv2.jsonapi.service.embedding;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.MeteredEmbeddingProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

/** Service to vectorize the data to embedding vector. */
@ApplicationScoped
public class DataVectorizerService {

  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public DataVectorizerService(
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  /**
   * This will vectorize the sort clause, update clause and the document with `$vectorize` field
   *
   * @param dataApiRequestInfo
   * @param commandContext
   * @param command
   * @return
   */
  public Uni<Command> vectorize(
      DataApiRequestInfo dataApiRequestInfo, CommandContext commandContext, Command command) {
    final DataVectorizer dataVectorizer =
        constructDataVectorizer(dataApiRequestInfo, commandContext);
    return vectorizeSortClause(dataVectorizer, commandContext, command)
        .onItem()
        .transformToUni(flag -> vectorizeDocument(dataVectorizer, commandContext, command))
        .onItem()
        .transform(flag -> command);
  }

  public DataVectorizer constructDataVectorizer(
      DataApiRequestInfo dataApiRequestInfo, CommandContext commandContext) {
    EmbeddingProvider embeddingProvider =
        Optional.ofNullable(commandContext.embeddingProvider())
            .map(
                provider ->
                    new MeteredEmbeddingProvider(
                        meterRegistry,
                        jsonApiMetricsConfig,
                        dataApiRequestInfo,
                        provider,
                        commandContext.commandName()))
            .orElse(null);
    return new DataVectorizer(
        embeddingProvider,
        objectMapper.getNodeFactory(),
        dataApiRequestInfo.getAndValidateEmbeddingApiKey(),
        commandContext.collectionSettings());
  }

  private Uni<Boolean> vectorizeSortClause(
      DataVectorizer dataVectorizer, CommandContext commandContext, Command command) {
    if (command instanceof Sortable sortable) {
      return dataVectorizer.vectorize(sortable.sortClause());
    }
    return Uni.createFrom().item(true);
  }

  private Uni<Boolean> vectorizeDocument(
      DataVectorizer dataVectorizer, CommandContext commandContext, Command command) {
    if (command instanceof InsertOneCommand insertOneCommand) {
      return dataVectorizer.vectorize(List.of(insertOneCommand.document()));
    } else if (command instanceof InsertManyCommand insertManyCommand) {
      return dataVectorizer.vectorize(insertManyCommand.documents());
    }
    return Uni.createFrom().item(true);
  }
}
