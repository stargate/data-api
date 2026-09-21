package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
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
  private static final String DROP_INDEX_CQL = "DROP INDEX IF EXISTS \"%s\".\"%s\";";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    logger.info("Executing DeleteCollectionCollectionOperation for {}", name);
    String keyspace = context.schemaObject().identifier().keyspace().asInternal();
    SimpleStatement dropTable =
        SimpleStatement.newInstance(DROP_TABLE_CQL.formatted(keyspace, name));

    return queryExecutor
        .getDriverMetadata(dataApiRequestInfo)
        .map(Metadata::getKeyspaces)
        .map(keyspaces -> keyspaces.get(context.schemaObject().identifier().keyspace()))
        .flatMap(
            keyspaceMetadata -> {
              if (keyspaceMetadata == null) {
                return dropTable(queryExecutor, dataApiRequestInfo, dropTable);
              }

              var table = keyspaceMetadata.getTable(CqlIdentifier.fromInternal(name)).orElse(null);
              if (table == null) {
                return dropTable(queryExecutor, dataApiRequestInfo, dropTable);
              }

              var collectionSettings =
                  CollectionSchemaObject.getCollectionSettings(
                      dataApiRequestInfo, table, OBJECT_MAPPER);
              if (!collectionSettings.openSearchDef().enabled()) {
                return dropTable(queryExecutor, dataApiRequestInfo, dropTable);
              }

              String saiIndexName = collectionSettings.openSearchDef().saiIndexName();
              SimpleStatement dropIndex =
                  SimpleStatement.newInstance(DROP_INDEX_CQL.formatted(keyspace, saiIndexName));
              return queryExecutor
                  .executeDropSchemaChange(dataApiRequestInfo, dropIndex)
                  .flatMap(ignored -> dropTable(queryExecutor, dataApiRequestInfo, dropTable));
            });
  }

  private static Uni<Supplier<CommandResult>> dropTable(
      QueryExecutor queryExecutor, RequestContext requestContext, SimpleStatement dropTable) {
    return queryExecutor
        .executeDropSchemaChange(requestContext, dropTable)
        .map(result -> new SchemaChangeResult(result.wasApplied()));
  }
}
