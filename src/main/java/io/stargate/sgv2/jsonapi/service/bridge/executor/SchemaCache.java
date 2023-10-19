package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;

/** Caches the vector enabled status for the keyspace */
@ApplicationScoped
public class SchemaCache {

  @Inject QueryExecutor queryExecutor;

  @Inject OperationsConfig operationsConfig;

  @Inject ObjectMapper objectMapper;

  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 10000;
  private final Cache<CacheKey, CollectionSettings> vectorCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public Uni<CollectionSettings> getCollectionSettings(
      Optional<String> tenant, String collectionName) {
    CacheKey key = new CacheKey(tenant, collectionName);
    CollectionSettings collectionSettings = vectorCache.getIfPresent(key);
    if (null != collectionSettings) {
      return Uni.createFrom().item(collectionSettings);
    } else {
      return getVectorProperties(key)
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
                  vectorCache.put(key, result);
                  return Uni.createFrom().item(result);
                }
              });
    }
  }

  private Uni<CollectionSettings> getVectorProperties(CacheKey cacheKey) {
    return queryExecutor
        .getSchema(operationsConfig.keyspace(), cacheKey.collection())
        .onItem()
        .transform(
            table -> {
              if (table.isPresent()) {
                return CollectionSettings.getCollectionSettings(table.get(), objectMapper);
              } else {
                throw new RuntimeException(
                    ErrorCode.INVALID_COLLECTION_NAME.getMessage() + cacheKey.collection());
              }
            });
  }

  record CacheKey(Optional<String> tenant, String collection) {}
}
