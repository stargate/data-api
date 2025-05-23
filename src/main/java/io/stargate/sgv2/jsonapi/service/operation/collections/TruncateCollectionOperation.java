package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the truncate collection.
 *
 * @param context Command context, carries namespace and the name of the collection.
 */
public record TruncateCollectionOperation(CommandContext<CollectionSchemaObject> context)
    implements Operation {
  private static final Logger logger = LoggerFactory.getLogger(TruncateCollectionOperation.class);
  private static final String TRUNCATE_TABLE_CQL = "TRUNCATE TABLE \"%s\".\"%s\";";

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    logger.info("Executing TruncateCollectionOperation for {}", context.schemaObject().name());
    String cql =
        TRUNCATE_TABLE_CQL.formatted(
            context.schemaObject().name().keyspace(), context.schemaObject().name().table());
    SimpleStatement query = SimpleStatement.newInstance(cql);
    // execute
    return queryExecutor
        .executeTruncateSchemaChange(dataApiRequestInfo, query)

        // if we have a result always respond positively
        .map(any -> new DeleteOperationPage(null, false, false, false));
  }
}
