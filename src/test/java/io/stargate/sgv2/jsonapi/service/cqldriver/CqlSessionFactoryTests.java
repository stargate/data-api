package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector.SubtypeOnlyFloatVectorToArrayCodec;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests for {@link CqlSessionFactory}. */
public class CqlSessionFactoryTests {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();

  private static final String APP_NAME = "appName" + System.currentTimeMillis();
  private static final String DATACENTER = "datacenter";
  private static final int CASSANDRA_PORT = 9042;

  @Test
  public void createAstraDbSession() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.<String>of();
    var fixture = newFixture(DatabaseType.ASTRA, endpoints, List.of(schemaListener));

    assertions(fixture, endpoints, schemaListener);
  }

  @Test
  public void createCassandraDbSession() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.<String>of("127.0.0.1", "127.0.0.2");
    var fixture = newFixture(DatabaseType.CASSANDRA, endpoints, List.of(schemaListener));

    assertions(fixture, endpoints, schemaListener);
  }

  @Test
  public void cassandraDBNeedsEndpoints() {

    var schemaListener = mock(SchemaChangeListener.class);
    var endpoints = List.<String>of();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              new CqlSessionFactory(
                  APP_NAME,
                  DatabaseType.CASSANDRA,
                  DATACENTER,
                  endpoints,
                  CASSANDRA_PORT,
                  List.of(schemaListener));
            });
    assertThat(ex)
        .as("Cassandra DB needs endpoints")
        .hasMessageContaining("but cassandraEndPoints is empty.");
  }

  private void assertions(
      Fixture fixture, List<String> endpoints, SchemaChangeListener schemaListener) {

    var actualSession = fixture.factory.apply(TEST_CONSTANTS.TENANT, fixture.credentials);
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
        .isEqualTo(TEST_CONSTANTS.TENANT.toString());

    verify(fixture.sessionBuilder).withApplicationName(APP_NAME);
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
    verify(fixture.sessionBuilder).build();
    verifyNoMoreInteractions(fixture.sessionBuilder);
  }

  record Fixture(
      CqlSessionBuilder sessionBuilder,
      CqlCredentials credentials,
      CqlSession session,
      CqlSessionFactory factory) {}

  private Fixture newFixture(
      DatabaseType databaseType,
      List<String> endpoints,
      List<SchemaChangeListener> schemaChangeListeners) {

    var session = mock(CqlSession.class);

    var sessionBuilder = mock(CqlSessionBuilder.class);
    when(sessionBuilder.withLocalDatacenter(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withClassLoader(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withConfigLoader(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withApplicationName(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.addSchemaChangeListener(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.withAuthCredentials(any(), any())).thenReturn(sessionBuilder);
    when(sessionBuilder.addContactPoints(any())).thenReturn(sessionBuilder);
    when(sessionBuilder.addTypeCodecs(any())).thenReturn(sessionBuilder);

    when(sessionBuilder.build()).thenReturn(session);

    var credentials = mock(CqlCredentials.class);
    when(credentials.addToSessionBuilder(any())).thenReturn(sessionBuilder);

    var factory =
        new CqlSessionFactory(
            APP_NAME,
            databaseType,
            DATACENTER,
            endpoints,
            CASSANDRA_PORT,
            schemaChangeListeners,
            () -> sessionBuilder);
    return new Fixture(sessionBuilder, credentials, session, factory);
  }
}
