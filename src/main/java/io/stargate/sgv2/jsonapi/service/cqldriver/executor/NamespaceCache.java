package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.schema.model.JsonapiTableMatcher;
import java.time.Duration;

/** Caches the vector enabled status for the namespace */
// TODO: what is the vector status of a namespace ? vectors are per collection
// TODO: clarify the name of this class, it is a cache of the collections/ tables not a cache of
// namespaces ??
public class NamespaceCache {

  public final String namespace;

  public final QueryExecutor queryExecutor;

  private final ObjectMapper objectMapper;

  private final boolean apiTablesEnabled;

  // TODO: move the settings to config
  // TODO: set the cache loader when creating the cache
  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 1000;
  private final Cache<String, SchemaObject> schemaObjectCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public NamespaceCache(String namespace, boolean apiTablesEnabled,
                        QueryExecutor queryExecutor, ObjectMapper objectMapper) {
    this.namespace = namespace;
    this.queryExecutor = queryExecutor;
    this.objectMapper = objectMapper;
    this.apiTablesEnabled = apiTablesEnabled;
  }

  protected Uni<SchemaObject> getSchemaObject(
      DataApiRequestInfo dataApiRequestInfo, String collectionName) {

    // TODO: why is this not using the loader pattern ?
    SchemaObject schemaObject = schemaObjectCache.getIfPresent(collectionName);

    if (null != schemaObject) {
      return Uni.createFrom().item(schemaObject);
    } else {
      return loadSchemaObject(dataApiRequestInfo, collectionName)
          .onItemOrFailure()
          .transformToUni(
              (result, error) -> {
                if (null != error) {
                  // not a valid collection schema
                  // TODO: Explain why this changes the error code
                  if (error instanceof JsonApiException
                      && ((JsonApiException) error).getErrorCode()
                          == ErrorCode.VECTORIZECONFIG_CHECK_FAIL) {
                    return Uni.createFrom()
                        .failure(
                            new JsonApiException(
                                ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA,
                                ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA
                                    .getMessage()
                                    .concat(collectionName)));
                  }
                  // collection does not exist
                  // TODO: DO NOT do a string starts with , use property error structures
                  // again, why is this here, looks like it returns the same error code ?
                  if (error instanceof RuntimeException rte
                      && rte.getMessage().startsWith(ErrorCode.COLLECTION_NOT_EXIST.getMessage())) {
                    return Uni.createFrom()
                        .failure(
                            new JsonApiException(
                                ErrorCode.COLLECTION_NOT_EXIST,
                                ErrorCode.COLLECTION_NOT_EXIST
                                    .getMessage()
                                    .concat(collectionName)));
                  }

                  // TODO This if block can be deleted? grpc code
                  // ignoring the error and return false. This will be handled while trying to
                  //  execute the query
                  // TODO: WHY ARE WE IGNORING THE ERROR AND RETURNING FAKE COLLECTION SCHEMA ? This
                  // is a bad practice
                  if ((error instanceof StatusRuntimeException sre
                      && (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND
                          || sre.getStatus().getCode() == io.grpc.Status.Code.INVALID_ARGUMENT))) {
                    return Uni.createFrom()
                        .item(
                            new CollectionSchemaObject(
                                namespace,
                                collectionName,
                                CollectionSchemaObject.IdConfig.defaultIdConfig(),
                                VectorConfig.notEnabledVectorConfig(),
                                null));
                  }
                  return Uni.createFrom().failure(error);
                } else {
                  schemaObjectCache.put(collectionName, result);
                  return Uni.createFrom().item(result);
                }
              });
    }
  }

  private Uni<SchemaObject> loadSchemaObject(
      DataApiRequestInfo dataApiRequestInfo, String collectionName) {

    return queryExecutor
        .getSchema(dataApiRequestInfo, namespace, collectionName)
        .onItem()
        .transform(
            optionalTable -> {
              // TODO: AARON - I changed the logic here, needs to be checked
              // TODO: error code here needs to be for collections and tables
              var table =
                  optionalTable.orElseThrow(
                      () ->
                          new RuntimeException(
                              ErrorCode.COLLECTION_NOT_EXIST.getMessage() + collectionName));

              // check if its a valid json api table
              // TODO: re-use the table matcher this is on the request hot path
              if (new JsonapiTableMatcher().test(table)) {
                return CollectionSchemaObject.getCollectionSettings(
                    optionalTable.get(), objectMapper);
              }

              if (apiTablesEnabled) {
                return new TableSchemaObject(namespace, collectionName);
              }

              // Target is not a collection and we are not supporting tables
              throw new JsonApiException(
                  ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA,
                  ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA.getMessage() + collectionName);
            });
  }

  public void evictCollectionSettingCacheEntry(String collectionName) {
    schemaObjectCache.invalidate(collectionName);
  }
}
