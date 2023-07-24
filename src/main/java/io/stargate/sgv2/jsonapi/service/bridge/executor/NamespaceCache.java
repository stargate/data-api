package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.time.Duration;

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
          .transform(
              (result, error) -> {
                if (null != error) {
                  if (error instanceof StatusRuntimeException sre
                      && sre.getStatus().equals(Status.UNAUTHENTICATED)) {
                    new RuntimeException(error);
                  }
                  throw new RuntimeException(
                      ErrorCode.INVALID_COLLECTION_NAME.getMessage() + collectionName);
                } else {
                  vectorCache.put(collectionName, result);
                  return result;
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
                throw new RuntimeException(ErrorCode.INVALID_COLLECTION_NAME + collectionName);
              }
            });
  }
}
