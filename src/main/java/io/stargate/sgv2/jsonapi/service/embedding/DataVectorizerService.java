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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Service to vectorize the data to embedding vector. */
@ApplicationScoped
public class DataVectorizerService {

  private final ObjectMapper objectMapper;

  @Inject
  public DataVectorizerService(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /**
   * This will vectorize the sort clause, update clause and the document with `$vectorize` field
   *
   * @param commandContext
   * @param command
   * @return
   */
  public Uni<Command> vectorize(CommandContext commandContext, Command command) {
    if (!commandContext.isVectorEnabled() || commandContext.embeddingService() == null)
      return Uni.createFrom().item(command);
    final DataVectorizer dataVectorizer =
        new DataVectorizer(commandContext.embeddingService(), objectMapper.getNodeFactory());
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
