package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import java.util.*;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POC Updates a Table using a CQL UPDATE statement.
 *
 * <p>TODO: marking POC because this is the first implementation.
 */
public class UpdateTableOperation extends TableMutationOperation {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTableOperation.class);

  private final UpdateValuesCQLClause updateValuesCQLClause;
  private final WhereCQLClause<Update> whereCQLClause;

  public UpdateTableOperation(
      CommandContext<TableSchemaObject> commandContext,
      UpdateValuesCQLClause updateValuesCQLClause,
      WhereCQLClause<Update> whereCQLClause) {
    super(commandContext);

    this.updateValuesCQLClause =
        Objects.requireNonNull(updateValuesCQLClause, "updateValuesCQLClause must not be null");
    this.whereCQLClause = Objects.requireNonNull(whereCQLClause, "whereCQLClause must not be null");
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // Start the select
    UpdateStart updateStart =
        update(
            commandContext.schemaObject().tableMetadata.getKeyspace(),
            commandContext.schemaObject().tableMetadata.getName());

    // Update the columns
    List<Object> positionalValues = new ArrayList<>();
    UpdateWithAssignments updateWithAssignments =
        updateValuesCQLClause.apply(updateStart, positionalValues);

    // HACK AARON - do not know how to get on the correct interface here
    Update update = (Update) updateWithAssignments;
    // Add the where clause
    update = whereCQLClause.apply(update, positionalValues);

    var statement = update.build(positionalValues.toArray());
    LOGGER.warn("UPDATE CQL: {}", update.asCql());
    LOGGER.warn("UPDATE VALUES: {}", positionalValues);

    return queryExecutor
        .executeWrite(dataApiRequestInfo, statement)
        .onItem()
        .transformToUni(UpdateTableOperation::getResultSupplier);
  }

  private static Uni<Supplier<CommandResult>> getResultSupplier(AsyncResultSet result) {
    return Uni.createFrom()
        .item(
            () -> {
              EnumMap<CommandStatus, Object> updateStatus = new EnumMap<>(CommandStatus.class);
              // Because CQL UPDATE is a upsert it will always match and always modify a row, even
              // if that means inserting
              // However - we do not know if an upsert happened :(
              updateStatus.put(CommandStatus.MATCHED_COUNT, 1);
              updateStatus.put(CommandStatus.MODIFIED_COUNT, 1);

              // need to return lambda to stop the Uni from unqrapping the supplier
              return () -> new CommandResult(null, updateStatus, List.of());
            });
  }
}
