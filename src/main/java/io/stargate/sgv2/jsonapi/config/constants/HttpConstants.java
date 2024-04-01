package io.stargate.sgv2.jsonapi.config.constants;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "stargate.jsonapi.http")
public interface HttpConstants {

  /** JSON API Authentication token header name. */
  String AUTHENTICATION_TOKEN_HEADER_NAME = "Token";

  /** JSON API also supports X-Cassandra-Token for backward compatibility. */
  String DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";

  /** JSON API Cassandra Authentication user name header name. */
  String AUTHENTICATION_USER_NAME_HEADER = "X-Cassandra-Username";

  /** JSON API Cassandra Authentication password header name. */
  String AUTHENTICATION_PASSWORD_HEADER = "X-Cassandra-Password";

  /** JSON API Embedding serive Authentication token header name. */
  String EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME = "x-embedding-api-key";

  /** @return Embedding service header name <code>20</code>. */
  @WithDefault(EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME)
  String embeddingApiKey();

  @WithDefault(AUTHENTICATION_USER_NAME_HEADER)
  String authenticationUserNameHeader();

  @WithDefault(AUTHENTICATION_PASSWORD_HEADER)
  String authenticationPasswordHeader();
}
