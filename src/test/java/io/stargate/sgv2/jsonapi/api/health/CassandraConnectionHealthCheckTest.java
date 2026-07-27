package io.stargate.sgv2.jsonapi.api.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlCredentials;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link CassandraConnectionHealthCheck}. */
public class CassandraConnectionHealthCheckTest {

  private static final UserAgent HEALTH_CHECK_USER_AGENT = new UserAgent("DataAPI-HealthCheck/1.0");
  private static final String USER_NAME = "test-user";
  private static final String PASSWORD = "test-password";

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private CQLSessionCache sessionCache;
  private OperationsConfig operationsConfig;
  private OperationsConfig.DatabaseConfig databaseConfig;
  private CqlSession session;
  private CassandraConnectionHealthCheck healthCheck;

  @BeforeEach
  public void setup() {
    TenantFactory.reset();
    TenantFactory.initialize(DatabaseType.CASSANDRA);

    sessionCache = mock(CQLSessionCache.class);
    session = mock(CqlSession.class);

    databaseConfig = mock(OperationsConfig.DatabaseConfig.class);
    when(databaseConfig.type()).thenReturn(DatabaseType.CASSANDRA);
    when(databaseConfig.fixedToken()).thenReturn(Optional.of(TEST_CONSTANTS.AUTH_TOKEN));
    when(databaseConfig.userName()).thenReturn(USER_NAME);
    when(databaseConfig.password()).thenReturn(PASSWORD);

    var consistencyConfig = mock(OperationsConfig.QueriesConfig.ConsistencyConfig.class);
    when(consistencyConfig.reads()).thenReturn(DefaultConsistencyLevel.LOCAL_QUORUM);

    var queriesConfig = mock(OperationsConfig.QueriesConfig.class);
    when(queriesConfig.consistency()).thenReturn(consistencyConfig);

    operationsConfig = mock(OperationsConfig.class);
    when(operationsConfig.databaseConfig()).thenReturn(databaseConfig);
    when(operationsConfig.queriesConfig()).thenReturn(queriesConfig);

    healthCheck =
        new CassandraConnectionHealthCheck(sessionCache, operationsConfig, Duration.ofMillis(100));
  }

  @AfterEach
  public void cleanup() {
    TenantFactory.reset();
  }

  @Test
  public void successfulHealthCheck() {
    var resultSet = mock(ResultSet.class);
    var row = mock(Row.class);

    sessionRequestReturns(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(false);
    when(session.execute(any(SimpleStatement.class))).thenReturn(resultSet);
    when(resultSet.one()).thenReturn(row);
    when(row.getString("release_version")).thenReturn("6.9.21");
    when(session.getName()).thenReturn("SINGLE-TENANT");

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
    assertThat(response.getData())
        .hasValueSatisfying(data -> assertThat(data).containsEntry("cassandra_version", "6.9.21"));
    assertThat(response.getData())
        .hasValueSatisfying(
            data -> assertThat(data).containsEntry("session_name", "SINGLE-TENANT"));

    var statementCaptor = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(session).execute(statementCaptor.capture());
    assertThat(statementCaptor.getValue().getQuery())
        .isEqualTo("SELECT release_version FROM system.local");
    assertThat(statementCaptor.getValue().getTimeout()).isEqualTo(Duration.ofMillis(100));
    assertThat(statementCaptor.getValue().getConsistencyLevel())
        .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
  }

  @Test
  public void sessionAcquisitionFailureReportsDown() {
    sessionRequestReturns(Uni.createFrom().failure(new IllegalStateException("Cannot connect")));

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .hasValueSatisfying(
            data ->
                assertThat(data)
                    .containsEntry("error", "IllegalStateException")
                    .containsEntry("message", "Cannot connect"));
    verifySessionNotEvicted();
  }

  @Test
  public void closedSessionReportsDownAndIsEvicted() {
    sessionRequestReturns(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(true);

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .hasValueSatisfying(data -> assertThat(data).containsEntry("reason", "Session is closed"));
    verifySessionEvicted();
    verify(session, never()).execute(any(SimpleStatement.class));
  }

  @Test
  public void queryFailureReportsDownAndUnreliableSessionIsEvicted() {
    sessionRequestReturns(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(false);
    when(session.execute(any(SimpleStatement.class)))
        .thenThrow(new ClosedConnectionException("Connection is closed"));

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .hasValueSatisfying(
            data ->
                assertThat(data)
                    .containsEntry("error", "ClosedConnectionException")
                    .containsEntry("message", "Connection is closed"));
    verifySessionEvicted();
  }

  @Test
  public void wrappedAllNodesFailedReportsDownAndUnreliableSessionIsEvicted() {
    var allNodesFailed = mock(AllNodesFailedException.class);

    sessionRequestReturns(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(false);
    when(session.execute(any(SimpleStatement.class)))
        .thenThrow(new RuntimeException("Session failed", allNodesFailed));

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .hasValueSatisfying(
            data ->
                assertThat(data)
                    .containsEntry("error", "RuntimeException")
                    .containsEntry("message", "Session failed"));
    verifySessionEvicted();
  }

  @Test
  public void queryFailureDoesNotEvictReliableSession() {
    sessionRequestReturns(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(false);
    when(session.execute(any(SimpleStatement.class)))
        .thenThrow(new IllegalStateException("Invalid query"));

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    verifySessionNotEvicted();
  }

  @Test
  public void sessionAcquisitionTimeoutReportsDown() {
    sessionRequestReturns(Uni.createFrom().nothing());

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    verifySessionNotEvicted();
    verifyNoInteractions(session);
  }

  @Test
  public void configuredCredentialsAreUsedWhenFixedTokenIsNotSet() {
    when(databaseConfig.fixedToken()).thenReturn(Optional.empty());
    healthCheck =
        new CassandraConnectionHealthCheck(sessionCache, operationsConfig, Duration.ofMillis(100));

    when(sessionCache.getSession(
            eq(TEST_CONSTANTS.CASSANDRA_TENANT), any(String.class), eq(HEALTH_CHECK_USER_AGENT)))
        .thenReturn(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(true);

    healthCheck.call();

    var expectedToken =
        CqlCredentials.USERNAME_PASSWORD_TOKEN_PREFIX
            + Base64.getEncoder().encodeToString(USER_NAME.getBytes(StandardCharsets.UTF_8))
            + ":"
            + Base64.getEncoder().encodeToString(PASSWORD.getBytes(StandardCharsets.UTF_8));
    verify(sessionCache)
        .getSession(TEST_CONSTANTS.CASSANDRA_TENANT, expectedToken, HEALTH_CHECK_USER_AGENT);
  }

  @Test
  public void nonCassandraDatabaseDoesNotRunConnectivityCheck() {
    when(databaseConfig.type()).thenReturn(DatabaseType.ASTRA);
    healthCheck =
        new CassandraConnectionHealthCheck(sessionCache, operationsConfig, Duration.ofMillis(100));

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
    assertThat(response.getData())
        .hasValueSatisfying(
            data ->
                assertThat(data)
                    .containsEntry(
                        "reason",
                        "Cassandra connectivity check is not applicable for database type ASTRA"));
    verifyNoInteractions(sessionCache);
  }

  private void sessionRequestReturns(Uni<CqlSession> sessionResult) {
    when(sessionCache.getSession(
            TEST_CONSTANTS.CASSANDRA_TENANT, TEST_CONSTANTS.AUTH_TOKEN, HEALTH_CHECK_USER_AGENT))
        .thenReturn(sessionResult);
  }

  private void verifySessionEvicted() {
    verify(sessionCache)
        .evictSession(
            TEST_CONSTANTS.CASSANDRA_TENANT, TEST_CONSTANTS.AUTH_TOKEN, HEALTH_CHECK_USER_AGENT);
  }

  private void verifySessionNotEvicted() {
    verify(sessionCache, never())
        .evictSession(
            TEST_CONSTANTS.CASSANDRA_TENANT, TEST_CONSTANTS.AUTH_TOKEN, HEALTH_CHECK_USER_AGENT);
  }
}
