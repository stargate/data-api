package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link UpdateOneCommand } */
@ApplicationScoped
public class UpdateManyCommandResolver extends FilterableResolver<UpdateManyCommand>
    implements CommandResolver<UpdateManyCommand> {
  private Shredder shredder;
  private final DocumentConfig documentConfig;

  @Inject
  public UpdateManyCommandResolver(
      ObjectMapper objectMapper, Shredder shredder, DocumentConfig documentConfig) {
    super(objectMapper);
    this.shredder = shredder;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<UpdateManyCommand> getCommandClass() {
    return UpdateManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, UpdateManyCommand command) {
    ReadOperation readOperation = resolve(ctx, command);
    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());
    boolean upsert = command.options() != null && command.options().upsert();
    return new ReadAndUpdateOperation(
        ctx,
        readOperation,
        documentUpdater,
        false,
        false,
        upsert,
        shredder,
        documentConfig.maxDocumentUpdateCount());
  }

  @Override
  protected FilteringOptions getFilteringOption(UpdateManyCommand command) {
    return new FilteringOptions(
        documentConfig.maxDocumentUpdateCount() + 1,
        null,
        documentConfig.defaultPageSize(),
        ReadType.DOCUMENT);
  }
}
