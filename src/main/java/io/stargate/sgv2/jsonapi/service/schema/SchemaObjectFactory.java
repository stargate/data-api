package io.stargate.sgv2.jsonapi.service.schema;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Creates
 */
public class SchemaObjectFactory implements SchemaObjectCache.SchemaObjectFactory {

  private static final CollectionTableMatcher IS_COLLECTION_PREDICATE =
      new CollectionTableMatcher();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final CQLSessionCache sessionCache;

  public SchemaObjectFactory(CQLSessionCache sessionCache) {
    this.sessionCache = Objects.requireNonNull(sessionCache, "sessionCache must not be null");
  }

  @Override
  public CompletionStage<SchemaObject> apply(
      RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh) {

    Objects.requireNonNull(requestContext, "requestContext must not be null");
    Objects.requireNonNull(identifier, "identifier must not be null");

    // sanity check
    if (! requestContext.tenant().equals(identifier.tenant())){
      throw new IllegalArgumentException("requestContext and identifier tenant mismatch, requestContext: %s, identifier: %s"
          .formatted(requestContext, identifier.tenant()));
    }

    Uni<? extends SchemaObject> uni =
        switch (identifier.type()) {
          case DATABASE -> createDatabaseSchemaObject(requestContext, identifier, forceRefresh);
          case KEYSPACE -> createKeyspaceSchemaObject(requestContext, identifier, forceRefresh);
          case TABLE ->
              createTableBasedSchemaObject(requestContext, identifier, forceRefresh)
                  .invoke(
                      tbso -> {
                        if (tbso.type() != SchemaObjectType.TABLE) {
                          throw new SchemaObjectTypeMismatchException(
                              SchemaObjectType.TABLE, tbso.type());
                        }
                      });
          case COLLECTION ->
              createTableBasedSchemaObject(requestContext, identifier, forceRefresh)
                  .invoke(
                      tbso -> {
                        if (tbso.type() != SchemaObjectType.COLLECTION) {
                          throw new SchemaObjectTypeMismatchException(
                              SchemaObjectType.COLLECTION, tbso.type());
                        }
                      });
          default ->
              throw new IllegalArgumentException(
                  "Unsupported schema object type: " + identifier.type());
        };
    return uni.map(obj -> (SchemaObject) obj).subscribeAsCompletionStage();
  }

  private Uni<DatabaseSchemaObject> createDatabaseSchemaObject(
      RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh) {

    // currently nothing to read for a database schema object
    return Uni.createFrom().item(() -> new DatabaseSchemaObject(identifier));
  }

  private Uni<KeyspaceSchemaObject> createKeyspaceSchemaObject(
      RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh) {

    // currently nothing to read for a keyspace schema object
    return getKeyspaceMetadata(requestContext, identifier, forceRefresh)
        .map(
            keyspaceMetadata ->
                new KeyspaceSchemaObject(requestContext.tenant(), keyspaceMetadata));
  }

  private Uni<TableBasedSchemaObject> createTableBasedSchemaObject(
      RequestContext requestContext, UnscopedSchemaObjectIdentifier scopedName, boolean forceRefresh) {

    return getKeyspaceMetadata(requestContext, scopedName, forceRefresh)
        .map(
            keyspaceMetadata -> {
              var tableMetadata =
                  keyspaceMetadata
                      .getTable(scopedName.objectName())
                      .orElseThrow(
                          () -> {
                            var allTables =
                                keyspaceMetadata.getTables().keySet().stream()
                                    .sorted(CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR)
                                    .map(CqlIdentifierUtil::cqlIdentifierToMessageString)
                                    .toList();
                            return SchemaException.Code.UNKNOWN_COLLECTION_OR_TABLE.get(
                                errVars(
                                    scopedName,
                                    vars -> vars.put("allTables", errFmtJoin(allTables))));
                          });
              return IS_COLLECTION_PREDICATE.test(tableMetadata)
                  ? CollectionSchemaObject.getCollectionSettings(
                      requestContext.tenant(), tableMetadata, OBJECT_MAPPER)
                  : TableSchemaObject.from(
                      requestContext.tenant(), tableMetadata, OBJECT_MAPPER);
            });
  }

  private Uni<KeyspaceMetadata> getKeyspaceMetadata(
      RequestContext requestContext, UnscopedSchemaObjectIdentifier scopedName, boolean forceRefresh) {

    var queryExecutor =
        new CommandQueryExecutor(
            sessionCache,
            new CommandQueryExecutor.DBRequestContext(requestContext),
            CommandQueryExecutor.QueryTarget.SCHEMA);

    return queryExecutor
        .getKeyspaceMetadata(scopedName.keyspace(), forceRefresh)
        .map(
            optKeyspace ->
                optKeyspace.orElseThrow(
                    () -> SchemaException.Code.UNKNOWN_KEYSPACE.get(errVars(scopedName))));
  }

  static class SchemaObjectTypeMismatchException extends RuntimeException {

    SchemaObjectTypeMismatchException(SchemaObjectType expected, SchemaObjectType actual) {
      super(
          String.format(
              "Expected schema object type %s but got %s", expected.apiName(), actual.apiName()));
    }
  }
}
