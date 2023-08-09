package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.time.Duration;
import java.util.Optional;

/** Caches the vector enabled status for the namespace */
public class NamespaceCache {

  public final String namespace;

  public final QueryExecutor queryExecutor;

  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 1000;
  private final Cache<String, CollectionProperty> vectorCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public NamespaceCache(String namespace, QueryExecutor queryExecutor) {
    this.namespace = namespace;
    this.queryExecutor = queryExecutor;
  }

  protected Uni<CollectionProperty> getCollectionProperties(String collectionName) {
    CollectionProperty collectionProperty = vectorCache.getIfPresent(collectionName);
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
                    return Uni.createFrom().item(new CollectionProperty(false, null));
                  }
                  return Uni.createFrom().failure(error);
                } else {
                  vectorCache.put(collectionName, result);
                  return Uni.createFrom().item(result);
                }
              });
    }
  }

  private Uni<CollectionProperty> getVectorProperties(String collectionName) {
    return queryExecutor
        .getSchema(namespace, collectionName)
        .onItem()
        .transform(
            table -> {
              if (table.isPresent()) {
                Boolean vectorEnabled =
                    table.get().getColumnsList().stream()
                        .anyMatch(
                            c ->
                                c.getName()
                                    .equals(
                                        DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME));
                if (vectorEnabled) {
                  final Optional<Schema.CqlIndex> vectorIndex =
                      table.get().getIndexesList().stream()
                          .filter(
                              i ->
                                  i.getColumnName()
                                      .equals(
                                          DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME))
                          .findFirst();
                  CollectionProperty.SimilarityFunction function =
                      CollectionProperty.SimilarityFunction.COSINE;
                  if (vectorIndex.isPresent()) {

                    if (vectorIndex
                        .get()
                        .getOptions()
                        .containsKey(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME)) {
                      function =
                          CollectionProperty.SimilarityFunction.fromString(
                              vectorIndex
                                  .get()
                                  .getOptions()
                                  .get(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME));
                    }
                  }
                  return new CollectionProperty(vectorEnabled, function);
                } else {
                  return new CollectionProperty(
                      vectorEnabled, CollectionProperty.SimilarityFunction.UNDEFINED);
                }
              } else {
                throw new RuntimeException(
                    ErrorCode.INVALID_COLLECTION_NAME.getMessage() + collectionName);
              }
            });
  }

  public record CollectionProperty(Boolean vectorEnabled, SimilarityFunction similarityFunction) {

    /**
     * The similarity function used for the vector index. This is only applicable if the vector
     * index is enabled.
     */
    public enum SimilarityFunction {
      COSINE,
      EUCLIDEAN,
      DOT_PRODUCT,
      UNDEFINED;

      public static SimilarityFunction fromString(String similarityFunction) {
        if (similarityFunction == null) return UNDEFINED;
        return switch (similarityFunction) {
          case "cosine" -> COSINE;
          case "euclidean" -> EUCLIDEAN;
          case "dot_product" -> DOT_PRODUCT;
          default -> throw new JsonApiException(
              ErrorCode.VECTOR_SEARCH_INVALID_FUCTION_NAME,
              ErrorCode.VECTOR_SEARCH_INVALID_FUCTION_NAME.getMessage() + similarityFunction);
        };
      }
    }
  }
}
