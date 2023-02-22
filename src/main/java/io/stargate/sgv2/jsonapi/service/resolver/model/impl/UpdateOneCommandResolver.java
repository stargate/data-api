package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
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
public class UpdateOneCommandResolver extends FilterableResolver<UpdateOneCommand>
    implements CommandResolver<UpdateOneCommand> {
  private Shredder shredder;

  @Inject
  public UpdateOneCommandResolver(ObjectMapper objectMapper, Shredder shredder) {
    super(objectMapper);
    this.shredder = shredder;
  }

  @Override
  public Class<UpdateOneCommand> getCommandClass() {
    return UpdateOneCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, UpdateOneCommand command) {
    ReadOperation readOperation = resolve(ctx, command);
    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());
    boolean upsert = command.options() != null && command.options().upsert();
    return new ReadAndUpdateOperation(
        ctx, readOperation, documentUpdater, false, false, upsert, shredder);
  }

  @Override
  protected FilteringOptions getFilteringOption(UpdateOneCommand command) {
    return new FilteringOptions(1, null, 1, ReadType.DOCUMENT);
  }
}
