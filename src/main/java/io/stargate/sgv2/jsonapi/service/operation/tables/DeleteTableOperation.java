package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** POC implementation of running a delete on a CQL table */
public class DeleteTableOperation extends TableMutationOperation {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableOperation.class);

  private final WhereCQLClause<Delete> whereCQLClause;

  public DeleteTableOperation(
      CommandContext<TableSchemaObject> commandContext, WhereCQLClause<Delete> whereCQLClause) {
    super(commandContext);
    this.whereCQLClause = Objects.requireNonNull(whereCQLClause, "whereCQLClause must not be null");
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // Start the delete
    DeleteSelection deleteSelection =
        deleteFrom(
            commandContext.schemaObject().tableMetadata().getKeyspace(),
            commandContext.schemaObject().tableMetadata().getName());
    // Delete in CQL allows you to delete columns, we do not support that, next to add is the WHERE
    // clause
    // HACK AARON - do not know how to get on the correct interface here
    Delete delete = (Delete) deleteSelection;
    List<Object> positionalValues = new ArrayList<>();
    delete = whereCQLClause.apply(delete, positionalValues);

    var statement = delete.build(positionalValues.toArray());

    LOGGER.warn("DELETE CQL: {}", delete.asCql());
    LOGGER.warn("DELETE VALUES: {}", positionalValues);

    // TODO HACK there is no way to know how many rows were deleted in CQL, cause it is a tombstone
    // insert
    // Collection uses the DeleteOperationPage, for now hack this to directly return the
    // CommandResult
    // until we work it out

    return queryExecutor
        .executeWrite(dataApiRequestInfo, statement)
        .onItem()
        .transformToUni(
            resultSet ->
                Uni.createFrom()
                    .item(
                        new Supplier<Supplier<CommandResult>>() {
                          @Override
                          public Supplier<CommandResult> get() {
                            return () ->
                                new CommandResult(
                                    null, Map.of(CommandStatus.DELETED_COUNT, -1), List.of());
                          }
                        }));
  }
}
