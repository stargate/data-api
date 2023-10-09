package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.time.Duration;

/** Caches the vector enabled status for the namespace */
public class NamespaceCache {

  public final String namespace;

  public final QueryExecutor queryExecutor;

  private final ObjectMapper objectMapper;

  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 1000;
  private final Cache<String, CollectionSettings> vectorCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public NamespaceCache(String namespace, QueryExecutor queryExecutor, ObjectMapper objectMapper) {
    this.namespace = namespace;
    this.queryExecutor = queryExecutor;
    this.objectMapper = objectMapper;
  }

  protected Uni<CollectionSettings> getCollectionProperties(String collectionName) {
    CollectionSettings collectionProperty = vectorCache.getIfPresent(collectionName);
    if (null != collectionProperty) {
      return Uni.createFrom().item(collectionProperty);
    } else {
      return getVectorProperties(collectionName)
          .onItemOrFailure()
          .transformToUni(
              (result, error) -> {
                if (null != error) {
                  // ignoring the error and return false. This will be handled while trying to
                  //  execute the query
                  if ((error instanceof StatusRuntimeException sre
                          && (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND
                              || sre.getStatus().getCode() == io.grpc.Status.Code.INVALID_ARGUMENT))
                      || (error instanceof RuntimeException rte
                          && rte.getMessage()
                              .startsWith(ErrorCode.INVALID_COLLECTION_NAME.getMessage()))) {
                    return Uni.createFrom()
                        .item(new CollectionSettings(collectionName, false, 0, null, null, null));
                  }
                  return Uni.createFrom().failure(error);
                } else {
                  vectorCache.put(collectionName, result);
                  return Uni.createFrom().item(result);
                }
              });
    }
  }

  private Uni<CollectionSettings> getVectorProperties(String collectionName) {
    return queryExecutor
        .getSchema(namespace, collectionName)
        .onItem()
        .transform(
            table -> {
              if (table.isPresent()) {
                return CollectionSettings.getVectorProperties(table.get(), objectMapper);
              } else {
                throw new RuntimeException(
                    ErrorCode.INVALID_COLLECTION_NAME.getMessage() + collectionName);
              }
            });
  }
}
