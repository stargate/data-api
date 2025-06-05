package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector.SubtypeOnlyFloatVectorToArrayCodec;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create {@link CqlSession} instances, normally used with the {@link CQLSessionCache}
 * via the {@link CQLSessionCache.SessionFactory} interface.
 *
 * <p>Abstracted out to make it easier to test the session cache and creating the session.
 */
public class CqlSessionFactory implements CQLSessionCache.SessionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CqlSessionFactory.class);

  private final String applicationName;

  private final String localDatacenter;
  private final Collection<InetSocketAddress> contactPoints;
  private final List<SchemaChangeListener> schemaChangeListeners;
  private final Supplier<CqlSessionBuilder> sessionBuilderSupplier;

  /**
   * Constructor for the CqlSessionFactory, normally this overload is used for non-testing code.
   *
   * @param applicationName the name of the application, set on the CQL session
   * @param localDatacenter the local datacenter for the client connection.
   * @param cassandraEndPoints the Cassandra endpoints, only used when the database type is
   *     CASSANDRA
   * @param cassandraPort the Cassandra port, only used when the database type is CASSANDRA
   * @param schemaChangeListeners the schema change listeners, these are added to the session to
   *     listen for schema changes from it.
   */
  CqlSessionFactory(
      String applicationName,
      String localDatacenter,
      List<String> cassandraEndPoints,
      Integer cassandraPort,
      List<SchemaChangeListener> schemaChangeListeners) {
    this(
        applicationName,
        localDatacenter,
        cassandraEndPoints,
        cassandraPort,
        schemaChangeListeners,
        CqlSessionBuilder::new);
  }

  /**
   * Constructor for the CqlSessionFactory, this overload is for testing so the SessionBuilder can
   * be mocked.
   *
   * @param applicationName the name of the application, set on the CQL session
   * @param localDatacenter the local datacenter for the client connection.
   * @param cassandraEndPoints the Cassandra endpoints, only used when the database type is
   *     CASSANDRA
   * @param cassandraPort the Cassandra port, only used when the database type is CASSANDRA
   * @param schemaChangeListeners the schema change listeners, these are added to the session to
   *     listen for schema changes from it.
   * @param sessionBuilderSupplier a supplier for creating CqlSessionBuilder instances, so that
   *     testing can mock the builder for session creation. In prod code use the ctor without this.
   */
  @VisibleForTesting
  CqlSessionFactory(
      String applicationName,
      String localDatacenter,
      List<String> cassandraEndPoints,
      Integer cassandraPort,
      List<SchemaChangeListener> schemaChangeListeners,
      Supplier<CqlSessionBuilder> sessionBuilderSupplier) {

    this.applicationName =
        Objects.requireNonNull(applicationName, "applicationName must not be null");
    if (applicationName.isBlank()) {
      throw new IllegalArgumentException("applicationName must not be blank");
    }
    this.localDatacenter =
        Objects.requireNonNull(localDatacenter, "localDatacenter must not be null");

    this.schemaChangeListeners =
        schemaChangeListeners == null ? List.of() : List.copyOf(schemaChangeListeners);
    this.sessionBuilderSupplier =
        Objects.requireNonNull(sessionBuilderSupplier, "sessionBuilderSupplier must not be null");

    // these never change, so we can cache
    // we cannot test if we need these to be provided until we create the session, because we do not
    // know the DB type until we know the tenant.
    contactPoints = cassandraEndPoints != null
        ? cassandraEndPoints.stream().map(host -> new InetSocketAddress(host, cassandraPort)).toList()
        : List.of();
  }

  @Override
  public CompletionStage<CqlSession> apply(Tenant tenant, CqlCredentials credentials) {
    Objects.requireNonNull(tenant, "tenant must not be null");
    Objects.requireNonNull(credentials, "credentials must not be null");

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Creating CQL Session tenant={}, credentials={}", tenant, credentials);
    }

    // the driver TypedDriverOption is only used with DriverConfigLoader.fromMap()
    // The ConfigLoader is held by the session and closed when the session closes, do not close it
    // here.
    // Setting the session name to the tenant, this is used by the driver to identify the session,
    // used in logging and metrics
    var configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.SESSION_NAME, tenant.toString())
            .build();

    var builder =
        sessionBuilderSupplier
            .get()
            .withLocalDatacenter(localDatacenter)
            .withClassLoader(Thread.currentThread().getContextClassLoader()) // TODO: EXPLAIN
            .withConfigLoader(configLoader)
            .withApplicationName(applicationName);

    for (var listener : schemaChangeListeners) {
      builder = builder.addSchemaChangeListener(listener);
    }
    builder = credentials.addToSessionBuilder(builder);

    // for astra it will default to 127.0.0.1 which is routed to the astra proxy
    if (tenant.databaseType() == DatabaseType.CASSANDRA) {
      if (contactPoints.isEmpty()) {
        throw new IllegalStateException(
            "Database type is %s but contactPoints is empty.".formatted(tenant.databaseType()));
      }
      builder = builder.addContactPoints(contactPoints);
    }

    // Add optimized CqlVector codec (see [data-api#1775])
    builder = builder.addTypeCodecs(SubtypeOnlyFloatVectorToArrayCodec.instance());

    // aaron - this used to have an if / else that threw an exception if the database type was not
    // known but we test that when creating the credentials for the cache key so no need to do it
    // here.
    return builder.buildAsync();
  }
}
