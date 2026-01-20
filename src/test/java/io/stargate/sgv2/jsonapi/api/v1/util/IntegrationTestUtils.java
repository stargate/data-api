package io.stargate.sgv2.jsonapi.api.v1.util;

/** Utilities for integration test. */
public final class IntegrationTestUtils {

  public static final String CASSANDRA_CQL_PORT_PROP = "stargate.int-test.cassandra.cql-port";
  public static final String CASSANDRA_USERNAME_PROP = "stargate.int-test.cassandra.username";
  public static final String CASSANDRA_PASSWORD_PROP = "stargate.int-test.cassandra.password";

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
}
