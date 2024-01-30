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
import java.util.List;

/** Service to vectorize the data for the embedding. */
@ApplicationScoped
public class DataVectorizerService {
  private final ObjectMapper objectMapper;

  public DataVectorizerService(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public Uni<Command> vectorize(CommandContext commandContext, Command command) {
    return vectorizeSortClause(commandContext, command)
        .onItem()
        .transform(flag -> vectorizeUpdateClause(commandContext, command))
        .onItem()
        .transform(flag -> vectorizeDocument(commandContext, command))
        .onItem()
        .transform(flag -> command);
  }

  private Uni<Boolean> vectorizeSortClause(CommandContext commandContext, Command command) {
    if (command instanceof Sortable sortable) {
      return commandContext.tryVectorize(objectMapper.getNodeFactory(), sortable.sortClause());
    }
    return Uni.createFrom().item(true);
  }

  private Uni<Boolean> vectorizeUpdateClause(CommandContext commandContext, Command command) {
    if (command instanceof Updatable updatable) {
      return commandContext.tryVectorize(objectMapper.getNodeFactory(), updatable.updateClause());
    }
    return Uni.createFrom().item(true);
  }

  private Uni<Boolean> vectorizeDocument(CommandContext commandContext, Command command) {
    if (command instanceof InsertOneCommand insertOneCommand) {
      return commandContext.tryVectorize(
          objectMapper.getNodeFactory(), List.of(insertOneCommand.document()));
    } else if (command instanceof InsertManyCommand insertManyCommand) {
      return commandContext.tryVectorize(
          objectMapper.getNodeFactory(), insertManyCommand.documents());
    } else if (command instanceof FindOneAndReplaceCommand findOneAndReplaceCommand) {
      return commandContext.tryVectorize(
          objectMapper.getNodeFactory(), List.of(findOneAndReplaceCommand.replacementDocument()));
    }
    return Uni.createFrom().item(true);
  }
}
