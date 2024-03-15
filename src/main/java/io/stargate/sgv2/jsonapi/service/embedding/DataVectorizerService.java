package io.stargate.sgv2.jsonapi.service.embedding;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
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
  private final DataApiRequestInfo dataApiRequestInfo;
  private final MeteredEmbeddingProvider meteredEmbeddingProvider;

  @Inject
  public DataVectorizerService(
      ObjectMapper objectMapper,
      DataApiRequestInfo dataApiRequestInfo,
      MeteredEmbeddingProvider meteredEmbeddingProvider) {
    this.objectMapper = objectMapper;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.meteredEmbeddingProvider = meteredEmbeddingProvider;
  }

  /**
   * This will vectorize the sort clause, update clause and the document with `$vectorize` field
   *
   * @param commandContext
   * @param command
   * @return
   */
  public Uni<Command> vectorize(CommandContext commandContext, Command command) {
    EmbeddingProvider embeddingProvider =
        Optional.ofNullable(commandContext.embeddingProvider())
            .map(
                provider ->
                    meteredEmbeddingProvider.setEmbeddingClient(
                        provider, command.getClass().getSimpleName()))
            .orElse(null);
    final DataVectorizer dataVectorizer =
        new DataVectorizer(
            embeddingProvider,
            objectMapper.getNodeFactory(),
            dataApiRequestInfo.getEmbeddingApiKey(),
            commandContext.collection());
    return vectorizeSortClause(dataVectorizer, commandContext, command)
        .onItem()
        .transformToUni(flag -> vectorizeUpdateClause(dataVectorizer, commandContext, command))
        .onItem()
        .transformToUni(flag -> vectorizeDocument(dataVectorizer, commandContext, command))
        .onItem()
        .transform(flag -> command);
  }

  private Uni<Boolean> vectorizeSortClause(
      DataVectorizer dataVectorizer, CommandContext commandContext, Command command) {
    if (command instanceof Sortable sortable) {
      return dataVectorizer.vectorize(sortable.sortClause());
    }
    return Uni.createFrom().item(true);
  }

  private Uni<Boolean> vectorizeUpdateClause(
      DataVectorizer dataVectorizer, CommandContext commandContext, Command command) {
    if (command instanceof Updatable updatable) {
      return dataVectorizer.vectorizeUpdateClause(updatable.updateClause());
    }
    return Uni.createFrom().item(true);
  }

  private Uni<Boolean> vectorizeDocument(
      DataVectorizer dataVectorizer, CommandContext commandContext, Command command) {
    if (command instanceof InsertOneCommand insertOneCommand) {
      return dataVectorizer.vectorize(List.of(insertOneCommand.document()));
    } else if (command instanceof InsertManyCommand insertManyCommand) {
      return dataVectorizer.vectorize(insertManyCommand.documents());
    } else if (command instanceof FindOneAndReplaceCommand findOneAndReplaceCommand) {
      return dataVectorizer.vectorize(List.of(findOneAndReplaceCommand.replacementDocument()));
    }
    return Uni.createFrom().item(true);
  }
}
