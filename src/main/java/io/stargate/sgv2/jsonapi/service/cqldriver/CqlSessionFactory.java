package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector.SubtypeOnlyFloatVectorToArrayCodec;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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

  private final DatabaseType databaseType;
  private final String localDatacenter;
  private final Collection<InetSocketAddress> contactPoints;
  private final Supplier<SchemaChangeListener> schemaChangeListenerSupplier;
  private final Supplier<CqlSessionBuilder> sessionBuilderSupplier;

  /**
   * Constructor for the CqlSessionFactory, normally this overload is used for non-testing code.
   *
   * @param applicationName the name of the application, set on the CQL session
   * @param databaseType the type of database, controls contact points and other settings
   * @param localDatacenter the local datacenter for the client connection.
   * @param cassandraEndPoints the Cassandra endpoints, only used when the database type is
   *     CASSANDRA
   * @param cassandraPort the Cassandra port, only used when the database type is CASSANDRA
   * @param schemaChangeListenerSupplier an optional supplier called to get a schema change listener
   *     for each new session created
   */
  CqlSessionFactory(
      String applicationName,
      DatabaseType databaseType,
      String localDatacenter,
      List<String> cassandraEndPoints,
      Integer cassandraPort,
      Supplier<SchemaChangeListener> schemaChangeListenerSupplier) {
    this(
        applicationName,
        databaseType,
        localDatacenter,
        cassandraEndPoints,
        cassandraPort,
        schemaChangeListenerSupplier,
        TenantAwareCqlSessionBuilder::new);
  }

  /**
   * Constructor for the CqlSessionFactory, this overload is for testing so the SessionBuilder can
   * be mocked.
   *
   * @param applicationName the name of the application, set on the CQL session
   * @param databaseType the type of database, controls contact points and other settings
   * @param localDatacenter the local datacenter for the client connection.
   * @param cassandraEndPoints the Cassandra endpoints, only used when the database type is
   *     CASSANDRA
   * @param cassandraPort the Cassandra port, only used when the database type is CASSANDRA
   * @param schemaChangeListenerSupplier an optional supplier called to get a schema change listener
   *     for each new session created
   * @param sessionBuilderSupplier a supplier for creating CqlSessionBuilder instances, so that
   *     testing can mock the builder for session creation. In prod code use the ctor without this.
   */
  @VisibleForTesting
  CqlSessionFactory(
      String applicationName,
      DatabaseType databaseType,
      String localDatacenter,
      List<String> cassandraEndPoints,
      Integer cassandraPort,
      Supplier<SchemaChangeListener> schemaChangeListenerSupplier,
      Supplier<CqlSessionBuilder> sessionBuilderSupplier) {

    this.applicationName =
        Objects.requireNonNull(applicationName, "applicationName must not be null");
    if (applicationName.isBlank()) {
      throw new IllegalArgumentException("applicationName must not be blank");
    }
    this.databaseType = Objects.requireNonNull(databaseType, "databaseType must not be null");
    this.localDatacenter =
        Objects.requireNonNull(localDatacenter, "localDatacenter must not be null");

    this.schemaChangeListenerSupplier = schemaChangeListenerSupplier;
    this.sessionBuilderSupplier =
        Objects.requireNonNull(sessionBuilderSupplier, "sessionBuilderSupplier must not be null");

    // these never change, and we do not have them in astra, so we can cache
    if (databaseType == DatabaseType.CASSANDRA) {
      Objects.requireNonNull(cassandraEndPoints, "cassandraEndPoints must not be null");
      if (cassandraEndPoints.isEmpty()) {
        throw new IllegalArgumentException(
            "Database type is %s but cassandraEndPoints is empty.".formatted(databaseType));
      }
      contactPoints =
          cassandraEndPoints.stream()
              .map(host -> new InetSocketAddress(host, cassandraPort))
              .toList();
    } else {
      contactPoints = List.of();
    }
  }

  @Override
  public CqlSession apply(String tenantId, CqlCredentials credentials) {
    Objects.requireNonNull(credentials, "credentials must not be null");

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Creating CQL Session tenantId={}, credentials={}", tenantId, credentials);
    }

    // the driver TypedDriverOption is only used with DriverConfigLoader.fromMap()
    // The ConfigLoader is held by the session and closed when the session closes, do not close it
    // here.
    // Setting the session name to the tenantId, this is used by the driver to identify the session,
    // used in logging and metrics
    var configLoader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.SESSION_NAME, tenantId == null ? "" : tenantId)
            .build();

    var builder =
        sessionBuilderSupplier
            .get()
            .withLocalDatacenter(localDatacenter)
            .withClassLoader(Thread.currentThread().getContextClassLoader()) // TODO: EXPLAIN
            .withConfigLoader(configLoader)
            .withApplicationName(applicationName);

    if (builder instanceof TenantAwareCqlSessionBuilder tenantAwareBuilder) {
      tenantAwareBuilder.withTenantId(tenantId);
    }

    if (null != schemaChangeListenerSupplier) {
      SchemaChangeListener listener = schemaChangeListenerSupplier.get();
      if (null == listener) {
        throw new IllegalStateException(
            "The schema change listener supplier returned a null listener.");
      }
      builder = builder.addSchemaChangeListener(listener);
    }

    builder = credentials.addToSessionBuilder(builder);

    // for astra it will default to 127.0.0.1 which is routed to the astra proxy
    if (databaseType == DatabaseType.CASSANDRA) {
      builder = builder.addContactPoints(contactPoints);
    }

    // Add optimized CqlVector codec (see [data-api#1775])
    builder = builder.addTypeCodecs(SubtypeOnlyFloatVectorToArrayCodec.instance());

    // aaron - this used to have an if / else that threw an exception if the database type was not
    // known but we test that when creating the credentials for the cache key so no need to do it
    // here.
    return builder.build();
  }
}
