package io.stargate.sgv2.jsonapi.api.health;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlCredentials;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Health check that verifies Cassandra connectivity for the Data API readiness probe.
 *
 * <p>The check obtains the Cassandra session used by normal requests from the session cache,
 * verifies that it is open, and executes a lightweight query against {@code system.local}. It is
 * only active when the Data API is configured with a {@link DatabaseType#CASSANDRA} backend.
 */
@Readiness
@ApplicationScoped
public class CassandraConnectionHealthCheck implements HealthCheck {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CassandraConnectionHealthCheck.class);

  private static final String HEALTH_CHECK_NAME = "cassandra-connection";
  private static final String HEALTH_CHECK_QUERY = "SELECT release_version FROM system.local";
  private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(5);
  private static final UserAgent HEALTH_CHECK_USER_AGENT = new UserAgent("DataAPI-HealthCheck/1.0");

  private final CQLSessionCache sessionCache;
  private final OperationsConfig operationsConfig;
  private final Duration timeout;
  private final String authToken;

  @Inject
  public CassandraConnectionHealthCheck(
      CqlSessionCacheSupplier sessionCacheSupplier, OperationsConfig operationsConfig) {
    this(
        Objects.requireNonNull(sessionCacheSupplier, "sessionCacheSupplier must not be null").get(),
        operationsConfig,
        HEALTH_CHECK_TIMEOUT);
  }

  @VisibleForTesting
  CassandraConnectionHealthCheck(
      CQLSessionCache sessionCache, OperationsConfig operationsConfig, Duration timeout) {
    this.sessionCache = Objects.requireNonNull(sessionCache, "sessionCache must not be null");
    this.operationsConfig =
        Objects.requireNonNull(operationsConfig, "operationsConfig must not be null");
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    this.authToken =
        operationsConfig.databaseConfig().type() == DatabaseType.CASSANDRA
            ? createAuthToken(operationsConfig.databaseConfig())
            : null;
  }

  @Override
  public HealthCheckResponse call() {
    var responseBuilder = HealthCheckResponse.named(HEALTH_CHECK_NAME);

    if (operationsConfig.databaseConfig().type() != DatabaseType.CASSANDRA) {
      return responseBuilder
          .up()
          .withData(
              "reason",
              "Cassandra connectivity check is not applicable for database type "
                  + operationsConfig.databaseConfig().type())
          .build();
    }

    var healthCheckTenant = TenantFactory.instance().create(null);

    try {
      var session =
          sessionCache
              .getSession(healthCheckTenant, authToken, HEALTH_CHECK_USER_AGENT)
              .await()
              .atMost(timeout);

      if (session.isClosed()) {
        LOGGER.warn("Cassandra session is closed during health check");
        evictSession(healthCheckTenant);
        return responseBuilder.down().withData("reason", "Session is closed").build();
      }

      var statement =
          SimpleStatement.builder(HEALTH_CHECK_QUERY)
              .setTimeout(timeout)
              .setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads())
              .build();

      var resultSet = session.execute(statement);
      var row = resultSet.one();
      var version = row != null ? row.getString("release_version") : "unknown";

      LOGGER.trace("Cassandra health check passed, version: {}", version);

      return responseBuilder
          .up()
          .withData("cassandra_version", version)
          .withData("session_name", session.getName())
          .build();
    } catch (Exception e) {
      if (isUnreliableSessionFailure(e)) {
        evictSession(healthCheckTenant);
      }

      LOGGER.error("Cassandra health check failed", e);
      return responseBuilder
          .down()
          .withData("error", e.getClass().getSimpleName())
          .withData("message", e.getMessage() != null ? e.getMessage() : "Unknown error")
          .build();
    }
  }

  private static String createAuthToken(OperationsConfig.DatabaseConfig databaseConfig) {
    return databaseConfig
        .fixedToken()
        .orElseGet(
            () ->
                CqlCredentials.USERNAME_PASSWORD_TOKEN_PREFIX
                    + encode(databaseConfig.userName())
                    + ":"
                    + encode(databaseConfig.password()));
  }

  private static String encode(String value) {
    return Base64.getEncoder()
        .encodeToString(
            Objects.requireNonNull(value, "Cassandra credential must not be null")
                .getBytes(StandardCharsets.UTF_8));
  }

  private static boolean isUnreliableSessionFailure(Throwable throwable) {
    var current = throwable;
    while (current != null) {
      if (current instanceof AllNodesFailedException
          || current instanceof ClosedConnectionException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void evictSession(Tenant tenant) {
    try {
      sessionCache.evictSession(tenant, authToken, HEALTH_CHECK_USER_AGENT);
    } catch (Exception e) {
      LOGGER.warn("Unable to evict the Cassandra session after a failed health check", e);
    }
  }
}
