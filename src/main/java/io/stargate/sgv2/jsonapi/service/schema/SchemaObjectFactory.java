package io.stargate.sgv2.jsonapi.service.schema;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingBinding;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingIndexValidator;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingTablePredicate;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaObjectFactory implements SchemaObjectCache.SchemaObjectFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaObjectFactory.class);

  private static final SuperShreddingTablePredicate IS_COLLECTION_PREDICATE =
      new SuperShreddingTablePredicate();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Supplier<CQLSessionCache> sessionCacheSupplier;

  public SchemaObjectFactory(Supplier<CQLSessionCache> sessionCacheSupplier) {
    this.sessionCacheSupplier =
        Objects.requireNonNull(sessionCacheSupplier, "sessionCacheSupplier must not be null");
  }

  @Override
  public CompletionStage<SchemaObject> apply(
      RequestContext requestContext, SchemaObjectIdentifier identifier, boolean forceRefresh) {

    Objects.requireNonNull(requestContext, "requestContext must not be null");
    Objects.requireNonNull(identifier, "identifier must not be null");

    // sanity check
    if (!requestContext.tenant().equals(identifier.tenant())) {
      throw new IllegalArgumentException(
          "requestContext and identifier tenant mismatch, requestContext: %s, identifier: %s"
              .formatted(requestContext, identifier.tenant()));
    }

    Uni<? extends SchemaObject> uni =
        switch (identifier.type()) {
          case DATABASE -> createDatabaseSchemaObject(requestContext, identifier, forceRefresh);
          case KEYSPACE -> createKeyspaceSchemaObject(requestContext, identifier, forceRefresh);
          case TABLE ->
              createTableBasedSchemaObject(requestContext, identifier, forceRefresh)
                  .onItem()
                  .transformToUni(
                      tableBasedSchemaObject -> {
                        if (tableBasedSchemaObject.type() != SchemaObjectType.TABLE) {
                          return Uni.createFrom()
                              .failure(
                                  new SchemaObjectTypeMismatchException(
                                      SchemaObjectType.TABLE, tableBasedSchemaObject.type()));
                        }
                        return Uni.createFrom().item(tableBasedSchemaObject);
                      });
          case COLLECTION ->
              createTableBasedSchemaObject(requestContext, identifier, forceRefresh)
                  .onItem()
                  .transformToUni(
                      tableBasedSchemaObject -> {
                        if (tableBasedSchemaObject.type() != SchemaObjectType.COLLECTION) {
                          return Uni.createFrom()
                              .failure(
                                  new SchemaObjectTypeMismatchException(
                                      SchemaObjectType.COLLECTION, tableBasedSchemaObject.type()));
                        }
                        return Uni.createFrom().item(tableBasedSchemaObject);
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
      RequestContext requestContext,
      UnscopedSchemaObjectIdentifier scopedName,
      boolean forceRefresh) {

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

              // clun: after checking the table metadata, we can determine if it's a collection or a table an
              return IS_COLLECTION_PREDICATE.test(tableMetadata)
                  ? validateAndCreateCollection(requestContext, tableMetadata)
                 : TableSchemaObject.from(requestContext.tenant(), tableMetadata, OBJECT_MAPPER);

              /* PROPOSAL
              return IS_COLLECTION_PREDICATE.test(tableMetadata)
                  ? CollectionSchemaObject.getCollectionSettings(
                      requestContext, tableMetadata, OBJECT_MAPPER)
                  : TableSchemaObject.from(requestContext.tenant(), tableMetadata, OBJECT_MAPPER);
               */
            });
  }

    private CollectionSchemaObject validateAndCreateCollection(
            RequestContext requestContext, TableMetadata tableMetadata) {

        // Validation of Indices before creating the CollectionSchemaObject.
        var bindingBuilder = SuperShreddingBinding.builder();
        var hasVector      = tableMetadata.getColumn(SuperShreddingMetadata.Names.QUERY_VECTOR_VALUE).isPresent();
        var hasLexical     = tableMetadata.getColumn(SuperShreddingMetadata.Names.QUERY_LEXICAL_VALUE).isPresent();
        if (hasVector)  bindingBuilder.withAnyVector();
        if (hasLexical) bindingBuilder.withAnyLexical();
        var validator = new SuperShreddingIndexValidator(bindingBuilder.build());

        // Warning is logged inside validate() -- no need to act on the result here yet.
        // todo clun - check the result of the validation and throw an exception if it fails
        SuperShreddingIndexValidator.ValidationResult validResult = validator.validate(tableMetadata);

        // Original code to create the CollectionSchemaObject after validation.
        return CollectionSchemaObject.getCollectionSettings(requestContext, tableMetadata, OBJECT_MAPPER);
    }

    private Uni<KeyspaceMetadata> getKeyspaceMetadata(
      RequestContext requestContext,
      UnscopedSchemaObjectIdentifier unscopedName,
      boolean forceRefresh) {

    var queryExecutor =
        new CommandQueryExecutor(
            sessionCacheSupplier.get(), requestContext, CommandQueryExecutor.QueryTarget.SCHEMA);

    //           .onFailure()
    //        .transform(
    //            throwable ->  {
    //              new DatabaseDriverExceptionHandler(new
    // DatabaseSchemaObject(requestContext.tenant()), null)
    //                  .maybeHandle(throwable);
    //            }
    //        )

    return queryExecutor
        .getKeyspaceMetadata(unscopedName.keyspace(), forceRefresh)
        .map(
            optKeyspace ->
                optKeyspace.orElseThrow(
                    () -> SchemaException.Code.UNKNOWN_KEYSPACE.get(errVars(unscopedName))));
  }

  static class SchemaObjectTypeMismatchException extends RuntimeException {

    SchemaObjectTypeMismatchException(SchemaObjectType expected, SchemaObjectType actual) {
      super(
          String.format(
              "Expected schema object type %s but got %s", expected.apiName(), actual.apiName()));
    }
  }
}
