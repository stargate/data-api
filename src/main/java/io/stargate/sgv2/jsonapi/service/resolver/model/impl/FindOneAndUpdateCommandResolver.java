package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.util.List;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneAndUpdateCommand } */
@ApplicationScoped
public class FindOneAndUpdateCommandResolver extends FilterableResolver<FindOneAndUpdateCommand>
    implements CommandResolver<FindOneAndUpdateCommand> {
  private final Shredder shredder;
  private final DocumentConfig documentConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public FindOneAndUpdateCommandResolver(
      ObjectMapper objectMapper, DocumentConfig documentConfig, Shredder shredder) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<FindOneAndUpdateCommand> getCommandClass() {
    return FindOneAndUpdateCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneAndUpdateCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);

    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());

    // resolve options
    FindOneAndUpdateCommand.Options options = command.options();
    boolean returnUpdatedDocument =
        options != null && "after".equals(command.options().returnDocument());
    boolean upsert = command.options() != null && command.options().upsert();

    // return
    return new ReadAndUpdateOperation(
        commandContext,
        findOperation,
        documentUpdater,
        true,
        returnUpdatedDocument,
        upsert,
        shredder,
        1,
        documentConfig.lwt().retries());
  }

  private FindOperation getFindOperation(
      CommandContext commandContext, FindOneAndUpdateCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    return new FindOperation(
        commandContext, filters, null, 1, 1, ReadType.DOCUMENT, Optional.empty(), objectMapper);
  }
}
