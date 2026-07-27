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
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
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

  private static final String FIXED_TOKEN = "fixed-token";
  private static final String USER_NAME = "test-user";
  private static final String PASSWORD = "test-password";

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
    when(databaseConfig.fixedToken()).thenReturn(Optional.of(FIXED_TOKEN));
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

    when(sessionCache.getSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().item(session));
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
    when(sessionCache.getSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("Cannot connect")));

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .hasValueSatisfying(
            data ->
                assertThat(data)
                    .containsEntry("error", "IllegalStateException")
                    .containsEntry("message", "Cannot connect"));
    verify(sessionCache, never())
        .evictSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class));
  }

  @Test
  public void closedSessionReportsDownAndIsEvicted() {
    when(sessionCache.getSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(true);

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    assertThat(response.getData())
        .hasValueSatisfying(data -> assertThat(data).containsEntry("reason", "Session is closed"));
    verify(sessionCache).evictSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class));
    verify(session, never()).execute(any(SimpleStatement.class));
  }

  @Test
  public void queryFailureReportsDownAndUnreliableSessionIsEvicted() {
    when(sessionCache.getSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().item(session));
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
    verify(sessionCache).evictSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class));
  }

  @Test
  public void wrappedAllNodesFailedReportsDownAndUnreliableSessionIsEvicted() {
    var allNodesFailed = mock(AllNodesFailedException.class);

    when(sessionCache.getSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().item(session));
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
    verify(sessionCache).evictSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class));
  }

  @Test
  public void queryFailureDoesNotEvictReliableSession() {
    when(sessionCache.getSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(false);
    when(session.execute(any(SimpleStatement.class)))
        .thenThrow(new IllegalStateException("Invalid query"));

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    verify(sessionCache, never())
        .evictSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class));
  }

  @Test
  public void sessionAcquisitionTimeoutReportsDown() {
    when(sessionCache.getSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().nothing());

    var response = healthCheck.call();

    assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
    verify(sessionCache, never())
        .evictSession(any(Tenant.class), eq(FIXED_TOKEN), any(UserAgent.class));
    verifyNoInteractions(session);
  }

  @Test
  public void configuredCredentialsAreUsedWhenFixedTokenIsNotSet() {
    when(databaseConfig.fixedToken()).thenReturn(Optional.empty());
    healthCheck =
        new CassandraConnectionHealthCheck(sessionCache, operationsConfig, Duration.ofMillis(100));

    when(sessionCache.getSession(any(Tenant.class), any(String.class), any(UserAgent.class)))
        .thenReturn(Uni.createFrom().item(session));
    when(session.isClosed()).thenReturn(true);

    healthCheck.call();

    var expectedToken =
        CqlCredentials.USERNAME_PASSWORD_TOKEN_PREFIX
            + Base64.getEncoder().encodeToString(USER_NAME.getBytes(StandardCharsets.UTF_8))
            + ":"
            + Base64.getEncoder().encodeToString(PASSWORD.getBytes(StandardCharsets.UTF_8));
    verify(sessionCache).getSession(any(Tenant.class), eq(expectedToken), any(UserAgent.class));
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
}
