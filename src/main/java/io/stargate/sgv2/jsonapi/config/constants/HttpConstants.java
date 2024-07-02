package io.stargate.sgv2.jsonapi.config.constants;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "stargate.jsonapi.http")
public interface HttpConstants {

  /** JSON API Authentication token header name. */
  String AUTHENTICATION_TOKEN_HEADER_NAME = "Token";

  /** JSON API also supports X-Cassandra-Token for backward compatibility. */
  String DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";

  /** JSON API Embedding serive Authentication token header name. */
  String EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME = "x-embedding-api-key";

  /** JSON API Embedding serive access id header name. */
  String EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME = "x-embedding-access-id";

  /** JSON API Embedding serive secret id header name. */
  String EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME = "x-embedding-secret-id";

  /**
   * @return Embedding service header name for token.
   */
  @WithDefault(EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME)
  String embeddingApiKey();

  /**
   * @return Embedding service header name for access id.
   */
  @WithDefault(EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME)
  String embeddingAccessId();

  /**
   * @return Embedding service header name for secret id.
   */
  @WithDefault(EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME)
  String embeddingSecretId();
}
