package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
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

  // 16-Oct-2025, tatu: [data-api#2230] Ensure ENV vars used as source too (see
  //
  // https://github.com/lightbend/config/blob/main/config/src/main/java/com/typesafe/config/ConfigFactory.java#L42
  //   -- ConfigFactory#OVERRIDE_WITH_ENV_PROPERTY_NAME -- which, alas, is `static private`
  //   so cannot refer from code.
  //
  // NOTE: actual overrides must use prefix "CONFIG_FORCE_" before modified property name.
  // Property names need to be modified so that
  //
  // * 1 underscore (_) represents dot "."
  // * 2 underscores (_) represents hyphen "-"
  // * 3 underscores (_) represents underscore "_"
  //
  // So, to override property for session name -- "datastax-java-driver.basic.session-name" --
  // We need to use env-var name of:
  //
  // "CONFIG_FORCE_" + "datastax__java__driver_" + "basic_session__name"
  // == "CONFIG_FORCE_datastax__java__driver_basic_session__name"
  static {
    final String PROP_KEY = "config.override_with_env_vars";
    LOGGER.info(
        "Setting system property '{}' to 'true' to enable ENV variable override for Cassandra Java Driver config",
        PROP_KEY);
    System.setProperty(PROP_KEY, "true");

    // But then let's also log overrides we have: Env Var and System Properties.
    // Driver will use these as overrides ultimately, over "application.conf" and "reference.conf",
    // but we will first log overrides.
    Config allOverrides = ConfigFactory.defaultOverrides();
    LOGGER.warn("Typesafe Config overrides for `cassandra-java-driver`:");
    LOGGER.warn("{}", allOverrides.root().render(ConfigRenderOptions.defaults().setJson(true)));

    // And let's also log effective configuration, under "datastax-java-driver"
    Config mergedConfig = ConfigFactory.load();
    LOGGER.warn(
        "Typesafe Config merged config for `cassandra-java-driver` (under '{}'):",
        DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
    LOGGER.warn(
        "{}",
        mergedConfig
            .getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH)
            .root()
            // Remove comments from "reference.conf", very verbose:
            .render(ConfigRenderOptions.defaults().setComments(false).setJson(true)));
  }

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
    // known, but we test that when creating the credentials for the cache key so no need to do it
    // here.
    return builder.build();
  }
}
