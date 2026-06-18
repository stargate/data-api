package io.stargate.sgv2.jsonapi.service.resolver.update;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.resolver.ClauseResolver;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;

/**
 * A {@link ClauseResolver} for the update clause in a command and creates a {@link
 * UpdateValuesCQLClause} for the operation to use.
 *
 * <p>For now, the only implementation is for Tables. Future work will also create an implementation
 * for Collections.
 *
 * @param <CmdT> The type of the command that has the update clause.
 * @param <SchemaT> The type of th schema object the command is running against.
 */
public abstract class UpdateResolver<CmdT extends Command & Updatable, SchemaT extends SchemaObject>
    extends ClauseResolver<CmdT, SchemaT, UpdateValuesCQLClause> {

  protected UpdateResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }
}
