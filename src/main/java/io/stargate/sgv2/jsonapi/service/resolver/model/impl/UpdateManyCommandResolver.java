package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link UpdateManyCommand } */
@ApplicationScoped
public class UpdateManyCommandResolver extends FilterableResolver<UpdateManyCommand>
    implements CommandResolver<UpdateManyCommand> {
  private final Shredder shredder;
  private final DocumentConfig documentConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public UpdateManyCommandResolver(
      ObjectMapper objectMapper, Shredder shredder, DocumentConfig documentConfig) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<UpdateManyCommand> getCommandClass() {
    return UpdateManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, UpdateManyCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);

    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());

    // resolve upsert
    UpdateManyCommand.Options options = command.options();
    boolean upsert = options != null && options.upsert();

    // return op
    return new ReadAndUpdateOperation(
        commandContext,
        findOperation,
        documentUpdater,
        false,
        false,
        upsert,
        shredder,
        DocumentProjector.identityProjector(),
        documentConfig.maxDocumentUpdateCount(),
        documentConfig.lwt().retries());
  }

  private FindOperation getFindOperation(CommandContext commandContext, UpdateManyCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    return new FindOperation(
        commandContext,
        filters,
        DocumentProjector.identityProjector(),
        null,
        documentConfig.maxDocumentUpdateCount() + 1,
        documentConfig.defaultPageSize(),
        ReadType.DOCUMENT,
        objectMapper);
  }
}
