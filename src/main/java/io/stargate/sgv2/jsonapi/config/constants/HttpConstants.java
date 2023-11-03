package io.stargate.sgv2.jsonapi.config.constants;

public interface HttpConstants {

  /** JSON API Authentication token header name. */
  String AUTHENTICATION_TOKEN_HEADER_NAME = "Token";

  /** JSON API also supports X-Cassandra-Token for backward compatibility. */
  String DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";
}
