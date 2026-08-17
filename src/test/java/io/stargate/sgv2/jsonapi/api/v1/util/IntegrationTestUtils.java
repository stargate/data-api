package io.stargate.sgv2.jsonapi.api.v1.util;

import java.util.Base64;

/** Utilities for integration test. */
public final class IntegrationTestUtils {

  public static final String CASSANDRA_CQL_HOST_PROP = "stargate.int-test.cassandra.host";
  public static final String CASSANDRA_CQL_PORT_PROP = "stargate.int-test.cassandra.cql-port";
  public static final String CASSANDRA_USERNAME_PROP = "stargate.int-test.cassandra.username";
  public static final String CASSANDRA_PASSWORD_PROP = "stargate.int-test.cassandra.password";
  public static final String AUTH_TOKEN_PROP = "stargate.int-test.auth-token";

  private IntegrationTestUtils() {}

  /**
   * @return Cassandra username, only meaningful if Cassandra auth is enabled
   */
  public static String getCassandraUsername() {
    return System.getProperty(CASSANDRA_USERNAME_PROP, "cassandra");
  }

  /**
   * @return Cassandra password, only meaningful if Cassandra auth is enabled
   */
  public static String getCassandraPassword() {
    return System.getProperty(CASSANDRA_PASSWORD_PROP, "cassandra");
  }

  /**
   * Value to send in the {@code Token} header of integration test requests.
   *
   * <p>Defaults to a Cassandra-style credential built from {@link #getCassandraUsername()} and
   * {@link #getCassandraPassword()}, which is what a containerized backend expects. When running
   * against an already-deployed Data API that validates real tokens (see the "Executing against a
   * running application" section of the README), set {@value #AUTH_TOKEN_PROP} and that value is
   * sent verbatim instead.
   *
   * @return Auth token to send, never null
   */
  public static String getAuthToken() {
    String token = System.getProperty(AUTH_TOKEN_PROP);
    if (token != null && !token.isBlank()) {
      return token;
    }
    return "Cassandra:"
        + Base64.getEncoder().encodeToString(getCassandraUsername().getBytes())
        + ":"
        + Base64.getEncoder().encodeToString(getCassandraPassword().getBytes());
  }
}
