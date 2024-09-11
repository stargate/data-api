package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the add index operation.
 *
 * @param context Command context, carries keyspace of the table.
 * @param columnName Name of the column to create the index on.
 * @param indexName Unique name for the index.
 */
public record AddIndexOperation(
    CommandContext<TableSchemaObject> context, String columnName, String indexName)
    implements Operation {
  private static final Logger logger = LoggerFactory.getLogger(AddIndexOperation.class);

  private static final String ADD_INDEX_TEMPLATE =
      "CREATE CUSTOM INDEX IF NOT EXISTS \"%s\" ON  %s.%s (%s) USING 'StorageAttachedIndex'";

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    logger.info(
        "Executing AddIndexOperation for {} {} {} {}",
        context.schemaObject().name().keyspace(),
        context.schemaObject().name().table(),
        columnName,
        indexName);
    String cql =
        ADD_INDEX_TEMPLATE.formatted(
            indexName,
            context.schemaObject().name().keyspace(),
            context.schemaObject().name().table(),
            columnName);
    SimpleStatement query = SimpleStatement.newInstance(cql);
    // execute
    return queryExecutor
        .executeDropSchemaChange(dataApiRequestInfo, query)

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }
}
