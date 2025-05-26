package io.stargate.sgv2.jsonapi.service.schema;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

public class SchemaObjectFactory implements SchemaObjectCache.SchemaObjectFactory {

  private static final CollectionTableMatcher IS_COLLECTION_PREDICATE = new CollectionTableMatcher();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final CQLSessionCache sessionCache;

  public SchemaObjectFactory(CQLSessionCache sessionCache) {
    this.sessionCache  = Objects.requireNonNull(sessionCache, "sessionCache must not be null");
  }

  @Override
  public CompletionStage<SchemaObject> apply(RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh) {

    Objects.requireNonNull(requestContext, "requestContext must not be null");
    Objects.requireNonNull(identifier, "identifier must not be null");

    Uni<? extends SchemaObject> uni = switch (identifier.type()) {
      case DATABASE -> createDatabaseSchemaObject(requestContext, identifier, forceRefresh);
      case KEYSPACE -> createKeyspaceSchemaObject(requestContext, identifier, forceRefresh);
      case TABLE -> createTableBasedSchemaObject(requestContext, identifier, forceRefresh)
          .invoke(
              tbso -> {
                if (tbso.type() != SchemaObjectType.TABLE){
                  throw new SchemaObjectTypeMismatchException(SchemaObjectType.TABLE, tbso.type());
                }}
          );
      case COLLECTION -> createTableBasedSchemaObject(requestContext, identifier, forceRefresh)
          .invoke(
              tbso -> {
                if (tbso.type() != SchemaObjectType.COLLECTION){
                  throw new SchemaObjectTypeMismatchException(SchemaObjectType.COLLECTION, tbso.type());
                }}
          );
      default ->
          throw new IllegalArgumentException(
              "Unsupported schema object type: " + identifier.type());
    };
    return uni
        .map(obj -> (SchemaObject)obj)
        .subscribeAsCompletionStage();
  }

  private Uni<DatabaseSchemaObject> createDatabaseSchemaObject(
      RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh) {

    // currently nothing to read for a database schema object
    return Uni.createFrom()
        .item(
            () ->
                new DatabaseSchemaObject(identifier));
  }

  private Uni<KeyspaceSchemaObject> createKeyspaceSchemaObject(
      RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh) {

    // currently nothing to read for a keyspace schema object
    return getKeyspaceMetadata(requestContext, identifier, forceRefresh)
        .map(keyspaceMetadata -> new KeyspaceSchemaObject(requestContext.getTenant(), keyspaceMetadata));
  }

  private Uni<TableBasedSchemaObject> createTableBasedSchemaObject(RequestContext requestContext, KeyspaceScopedName scopedName, boolean forceRefresh) {

    return getKeyspaceMetadata(requestContext, scopedName, forceRefresh)
        .map(keyspaceMetadata -> {
          var tableMetadata = keyspaceMetadata
              .getTable(scopedName.objectName())
              .orElseThrow(
                  () -> {
                    // TODO: XXX / ERROR
                    return new RuntimeException("BANG XXX");
                  }
              );

          return IS_COLLECTION_PREDICATE.test(tableMetadata)
              ? CollectionSchemaObject.getCollectionSettings(requestContext.getTenant(), tableMetadata, OBJECT_MAPPER)
              : TableSchemaObject.from(requestContext.getTenant(), tableMetadata, OBJECT_MAPPER);
        });
  }

  private Uni<KeyspaceMetadata> getKeyspaceMetadata(RequestContext requestContext, KeyspaceScopedName scopedName, boolean forceRefresh) {

    var queryExecutor = new CommandQueryExecutor(sessionCache,
        new CommandQueryExecutor.DBRequestContext(requestContext),
        CommandQueryExecutor.QueryTarget.SCHEMA);

    return queryExecutor
        .getKeyspaceMetadata(scopedName.keyspace(), forceRefresh)
        .map(optKeyspace ->
            optKeyspace.orElseThrow(() -> {
              // TODO: XXX / ERROR
              return new RuntimeException("BANG XXX");
            }));
  }

  static class SchemaObjectTypeMismatchException extends RuntimeException {

    SchemaObjectTypeMismatchException(SchemaObjectType expected, SchemaObjectType actual) {
      super(
          String.format(
              "Expected schema object type %s but got %s", expected.apiName(), actual.apiName()));
    }
  }
}
