package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the delete collection.
 *
 * @param context Command context, carries namespace of the collection.
 * @param name Collection name.
 */
public record DeleteCollectionCollectionOperation(
    CommandContext<KeyspaceSchemaObject> context, String name) implements Operation {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteCollectionCollectionOperation.class);

  private static final String DROP_TABLE_CQL = "DROP TABLE IF EXISTS \"%s\".\"%s\";";

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    logger.info("Executing DeleteCollectionCollectionOperation for {}", name);
    String cql = DROP_TABLE_CQL.formatted(context.schemaObject().identifier().keyspace(), name);
    SimpleStatement query = SimpleStatement.newInstance(cql);
    // execute
    return queryExecutor
        .executeDropSchemaChange(dataApiRequestInfo, query)

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }
}
