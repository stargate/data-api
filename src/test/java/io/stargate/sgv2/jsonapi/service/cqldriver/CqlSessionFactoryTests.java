package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandlerTest.mockNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.ExceptionFlags;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector.SubtypeOnlyFloatVectorToArrayCodec;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link CqlSessionFactory}. */
public class CqlSessionFactoryTests {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private static final String DATACENTER = "datacenter";
  private static final int CASSANDRA_PORT = 9042;

  @Test
  public void createAstraDbSession() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.<String>of();
    var fixture = newFixture(TEST_CONSTANTS.TENANT, endpoints, schemaListener);

    assertions(fixture, endpoints, schemaListener);
  }

  @Test
  public void createAstraDbSessionAuthenticationError() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.<String>of();

    var localHost = InetSocketAddress.createUnresolved("localhost", 9042);
    var authErr =
        new AuthenticationException(
            new DefaultEndPoint(localHost), "A FAKE authentication error occurred");

    var node = mockNode("node: " + localHost);
    var allNodesErr =
        AllNodesFailedException.fromErrors(List.of(new AbstractMap.SimpleEntry<>(node, authErr)));

    var fixture =
        newFixture(TEST_CONSTANTS.TENANT, endpoints, schemaListener, allNodesErr, true, null);

    // because the CqlSessionFactory is working through java CompletionStage the exception
    // the cause is smuggled out in CompletionException
    assertThatThrownBy(() -> assertions(fixture, endpoints, schemaListener))
        .as("Authentication error from driver mapped")
        .isInstanceOfSatisfying(
            CompletionException.class,
            completionException -> {
              assertThat(completionException.getCause()).isInstanceOf(APISecurityException.class);
              APISecurityException err = (APISecurityException) completionException.getCause();
              assertThat(err.code)
                  .isEqualTo(APISecurityException.Code.UNAUTHENTICATED_REQUEST.name());
              // this is one of the few situations where we return non HTTP 200
              assertThat(err.httpStatus).as("Authentication error is HTTP 401").isEqualTo(401);
            });
  }

  @Test
  public void createAstraDbSessionMissingMetadata() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.<String>of();

    var fixture = newFixture(TEST_CONSTANTS.TENANT, endpoints, schemaListener, null, false, null);

    // because the CqlSessionFactory is working through java CompletionStage the exception
    // the cause is smuggled out in CompletionException
    assertThatThrownBy(() -> assertions(fixture, endpoints, schemaListener))
        .as("Authentication error from driver mapped")
        .isInstanceOfSatisfying(
            CompletionException.class,
            completionException -> {
              assertThat(completionException.getCause()).isInstanceOf(DatabaseException.class);
              DatabaseException err = (DatabaseException) completionException.getCause();
              assertThat(err.code).isEqualTo(DatabaseException.Code.FAILED_TO_READ_METADATA.name());
              // this is one of the few situations where we return non HTTP 200
              assertThat(err.exceptionFlags)
                  .as("Exception flagged as UNRELIABLE_DB_SESSION")
                  .contains(ExceptionFlags.UNRELIABLE_DB_SESSION);
            });

    // confirming we are correctly closing the session if metadata is missing
    verify(fixture.session()).closeAsync();
  }

  @Test
  public void createAstraDbSessionMissingMetadataErrorClosing() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.<String>of();

    var closingError = new RuntimeException("FAKE closeAsync() exception");
    var fixture =
        newFixture(TEST_CONSTANTS.TENANT, endpoints, schemaListener, null, false, closingError);

    // same asserts as createAstraDbSessionMissingMetadata - but this is checking that even if
    // closeAsync() throws
    // the returned error to the user is FAILED_TO_READ_METADATA
    assertThatThrownBy(() -> assertions(fixture, endpoints, schemaListener))
        .as("Authentication error from driver mapped")
        .isInstanceOfSatisfying(
            CompletionException.class,
            completionException -> {
              assertThat(completionException.getCause()).isInstanceOf(DatabaseException.class);
              DatabaseException err = (DatabaseException) completionException.getCause();
              assertThat(err.code).isEqualTo(DatabaseException.Code.FAILED_TO_READ_METADATA.name());
              // this is one of the few situations where we return non HTTP 200
              assertThat(err.exceptionFlags)
                  .as("Exception flagged as UNRELIABLE_DB_SESSION")
                  .contains(ExceptionFlags.UNRELIABLE_DB_SESSION);
            });

    // confirming we are correctly closing the session if metadata is missing
    verify(fixture.session()).closeAsync();
  }

  @Test
  public void createCassandraDbSession() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.of("127.0.0.1", "127.0.0.2");
    var fixture = newFixture(TEST_CONSTANTS.CASSANDRA_TENANT, endpoints, schemaListener);

    assertions(fixture, endpoints, schemaListener);
  }

  private void assertions(
      Fixture fixture, List<String> endpoints, SchemaChangeListener schemaListener) {

    var actualSession =
        fixture.factory.apply(fixture.tenant, fixture.credentials).toCompletableFuture().join();

    assertThat(actualSession)
        .as("session is same as returned from session builder")
        .isSameAs(fixture.session);

    verify(fixture.sessionBuilder).withLocalDatacenter(DATACENTER);
    verify(fixture.sessionBuilder).withClassLoader(Thread.currentThread().getContextClassLoader());

    // tenantID should be used as the sessionName, to get that we need to capture the config loader
    var captor = ArgumentCaptor.forClass(DriverConfigLoader.class);
    verify(fixture.sessionBuilder).withConfigLoader(captor.capture());

    // This only checks that the sessionName is set on the config loader
    // Integration Tests will check for the session_cql_requests_seconds_bucket and that the session
    // tag is set
    var defaultDriverProfile = captor.getValue().getInitialConfig().getDefaultProfile();
    assertThat(defaultDriverProfile.isDefined(DefaultDriverOption.SESSION_NAME))
        .as("sessionName set on profile")
        .isTrue();
    assertThat(defaultDriverProfile.getString(DefaultDriverOption.SESSION_NAME))
        .as("sessionName set to tenantId")
        .isEqualTo(fixture.tenant.toString());

    verify(fixture.sessionBuilder).withApplicationName(TEST_CONSTANTS.APP_NAME);
    verify(fixture.sessionBuilder).addSchemaChangeListener(schemaListener);

    // verifying it called the CqlCredentials to add to the session not how they were added to the
    // session
    verify(fixture.credentials).addToSessionBuilder(fixture.sessionBuilder);

    // no contact points set for astra
    verify(fixture.sessionBuilder, never()).addContactEndPoints(any());
    verify(fixture.sessionBuilder).addTypeCodecs(SubtypeOnlyFloatVectorToArrayCodec.instance());

    if (!endpoints.isEmpty()) {
      var expectedEndpoints =
          endpoints.stream().map(host -> new InetSocketAddress(host, CASSANDRA_PORT)).toList();
      verify(fixture.sessionBuilder).addContactPoints(expectedEndpoints);
    }

    // this is going to be called once only.
    verify(fixture.sessionBuilder).buildAsync();
    verifyNoMoreInteractions(fixture.sessionBuilder);
  }

  record Fixture(
      Tenant tenant,
      CqlSessionBuilder sessionBuilder,
      CqlCredentials credentials,
      CqlSession session,
      CqlSessionFactory factory) {}

  private Fixture newFixture(
      Tenant tenant, List<String> endpoints, SchemaChangeListener schemaChangeListener) {
    return newFixture(tenant, endpoints, schemaChangeListener, null, true, null);
  }

  private Fixture newFixture(
      Tenant tenant,
      List<String> endpoints,
      SchemaChangeListener schemaChangeListener,
      RuntimeException error,
      boolean withMetadata,
      RuntimeException closingError) {

    // we are testing that the CqlSessionFactory calls the session builder correctly,
    // so we mock the session builder and verify that it is called correctly.
    var session = mock(CqlSession.class);

    // CqlSession guarantees a Metdata obj, and we now check it has keyspaces
    var metadata = mock(Metadata.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspaces())
        .thenReturn(
            withMetadata
                ? Map.of(CqlIdentifier.fromInternal("system"), mock(KeyspaceMetadata.class))
                : Map.of());
    if (closingError == null) {
      when(session.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
    } else {
      when(session.closeAsync()).thenReturn(CompletableFuture.failedFuture(closingError));
    }

    var sessionBuilder = mock(CqlSessionBuilder.class);
    when(sessionBuilder.withLocalDatacenter(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withClassLoader(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withConfigLoader(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withApplicationName(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.addSchemaChangeListener(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withAuthCredentials(any(), any())).thenReturn(sessionBuilder);
    when(sessionBuilder.addContactPoints(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.addTypeCodecs(any())).thenReturn(sessionBuilder);

    if (error != null) {
      // when the driver completes any error is generates is wrapped in
      // CompletionException() because the thown error has passed through stages.
      // wrapping here to make it the same
      when(sessionBuilder.buildAsync())
          .thenReturn(CompletableFuture.failedFuture(new CompletionException(error)));
    } else {
      when(sessionBuilder.buildAsync()).thenReturn(CompletableFuture.completedFuture(session));
    }
    var credentials = mock(CqlCredentials.class);
    when(credentials.addToSessionBuilder(any())).thenReturn(sessionBuilder);

    var factory =
        new CqlSessionFactory(
            TEST_CONSTANTS.APP_NAME,
            DATACENTER,
            endpoints,
            CASSANDRA_PORT,
            schemaChangeListener != null ? () -> schemaChangeListener : null,
            () -> sessionBuilder);
    return new Fixture(tenant, sessionBuilder, credentials, session, factory);
  }
}
