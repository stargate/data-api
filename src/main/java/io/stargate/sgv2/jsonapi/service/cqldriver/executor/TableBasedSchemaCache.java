package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * TODO: @YUQI - DELETE WHEN SCHMEA OBJECT CACHE IS READY
 *
 * <p>Caches the vector enabled status for the namespace
 */
// TODO: what is the vector status of a namespace ? vectors are per collection
// TODO: clarify the name of this class, it is a cache of the collections/ tables not a cache of
// namespaces ??
public class TableBasedSchemaCache {

  public final String namespace;

  public final QueryExecutor queryExecutor;

  private final ObjectMapper objectMapper;

  // TODO: move the settings to config
  // TODO: set the cache loader when creating the cache
  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 1000;
  private final Cache<String, SchemaObject> schemaObjectCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public TableBasedSchemaCache(
      String namespace, QueryExecutor queryExecutor, ObjectMapper objectMapper) {
    this.namespace = namespace;
    this.queryExecutor = queryExecutor;
    this.objectMapper = objectMapper;
  }

  protected Uni<SchemaObject> getSchemaObject(
      RequestContext requestContext, String collectionName, boolean forceRefresh) {

    // TODO: why is this not using the loader pattern ?
    SchemaObject schemaObject = null;
    if (!forceRefresh) {
      schemaObject = schemaObjectCache.getIfPresent(collectionName);
    }
    if (null != schemaObject) {
      return Uni.createFrom().item(schemaObject);
    } else {
      return loadSchemaObject(requestContext, collectionName)
          .onItemOrFailure()
          .transformToUni(
              (result, error) -> {
                if (null != error) {
                  // not a valid collection schema
                  // TODO: Explain why this changes the error code
                  if (error instanceof JsonApiException
                      && ((JsonApiException) error).getErrorCode()
                          == ErrorCodeV1.VECTORIZECONFIG_CHECK_FAIL) {
                    return Uni.createFrom()
                        .failure(
                            ErrorCodeV1.INVALID_JSONAPI_COLLECTION_SCHEMA.toApiException(
                                "%s", collectionName));
                  }
                  // collection does not exist
                  // TODO: DO NOT do a string starts with, use proper error structures
                  // again, why is this here, looks like it returns the same error code ?
                  // Guess: this a driver exception, not Data API's internal one
                  if (error instanceof RuntimeException rte
                      && rte.getMessage().startsWith("Collection does not exist")) {
                    return Uni.createFrom()
                        .failure(
                            SchemaException.Code.COLLECTION_NOT_EXIST.get(
                                Map.of("collection", collectionName)));
                  }
                  return Uni.createFrom().failure(error);
                }
                schemaObjectCache.put(collectionName, result);
                return Uni.createFrom().item(result);
              });
    }
  }

  Optional<SchemaObject> peekSchemaObject(String tableName) {
    return Optional.ofNullable(schemaObjectCache.getIfPresent(tableName));
  }

  private Uni<SchemaObject> loadSchemaObject(RequestContext requestContext, String collectionName) {

    return queryExecutor
        .getTableMetadata(requestContext, namespace, collectionName)
        .onItem()
        .transform(
            optionalTable -> {
              // TODO: AARON - I changed the logic here, needs to be checked
              // TODO: error code here needs to be for collections and tables
              var table =
                  optionalTable.orElseThrow(
                      () ->
                          SchemaException.Code.COLLECTION_NOT_EXIST.get(
                              Map.of("collection", collectionName)));

              // check if its a valid json API Table
              // TODO: re-use the table matcher this is on the request hot path
              if (new CollectionTableMatcher().test(table)) {
                return CollectionSchemaObject.getCollectionSettings(
                    optionalTable.get(), objectMapper);
              }

              return TableSchemaObject.from(table, objectMapper);
            });
  }

  public void evictCollectionSettingCacheEntry(String collectionName) {
    schemaObjectCache.invalidate(collectionName);
  }
}
