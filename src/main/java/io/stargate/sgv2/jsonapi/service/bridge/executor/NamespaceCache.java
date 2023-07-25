package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.time.Duration;

/** Caches the vector enabled status for the namespace */
public class NamespaceCache {

  public final String namespace;

  public final QueryExecutor queryExecutor;

  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 1000;
  private final Cache<String, Boolean> vectorCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public NamespaceCache(String namespace, QueryExecutor queryExecutor) {
    this.namespace = namespace;
    this.queryExecutor = queryExecutor;
  }

  protected Uni<Boolean> isVectorEnabled(String collectionName) {
    Boolean vectorEnabled = vectorCache.getIfPresent(collectionName);
    if (null != vectorEnabled) {
      return Uni.createFrom().item(vectorEnabled);
    } else {
      return isVectorEnabledInternal(collectionName)
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
                    return Uni.createFrom().item(false);
                  }
                  return Uni.createFrom().failure(error);
                } else {
                  vectorCache.put(collectionName, result);
                  return Uni.createFrom().item(result);
                }
              });
    }
  }

  private Uni<Boolean> isVectorEnabledInternal(String collectionName) {
    return queryExecutor
        .getSchema(namespace, collectionName)
        .onItem()
        .transform(
            table -> {
              if (table.isPresent()) {
                return table.get().getColumnsList().stream()
                    .anyMatch(
                        c ->
                            c.getName()
                                .equals(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME));
              } else {
                throw new RuntimeException(
                    ErrorCode.INVALID_COLLECTION_NAME.getMessage() + collectionName);
              }
            });
  }
}
