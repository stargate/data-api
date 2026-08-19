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
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.ExceptionFlags;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.optvector.SubtypeOnlyFloatVectorToArrayCodec;
import io.stargate.sgv2.jsonapi.service.operation.databases.DatabaseDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
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
    LOGGER.warn(
        "Typesafe Config overrides for `cassandra-java-driver`: {}",
        allOverrides.root().render(ConfigRenderOptions.defaults().setJson(true)));

    // And let's also log effective configuration, under "datastax-java-driver"
    Config mergedConfig = ConfigFactory.load();
    LOGGER.warn(
        "Typesafe Config merged config for `cassandra-java-driver` (under '{}'): {}",
        DefaultDriverConfigLoader.DEFAULT_ROOT_PATH,
        mergedConfig
            .getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH)
            .root()
            // Remove comments from "reference.conf", very verbose:
            .render(ConfigRenderOptions.defaults().setComments(false).setJson(true)));
  }

  private final String applicationName;

  private final String localDatacenter;
  private final Collection<InetSocketAddress> contactPoints;
  private final Supplier<SchemaChangeListener> schemaChangeListenerSupplier;
  private final Supplier<CqlSessionBuilder> sessionBuilderSupplier;

  /**
   * Constructor for the CqlSessionFactory, normally this overload is used for non-testing code.
   *
   * @param applicationName the name of the application, set on the CQL session
   * @param localDatacenter the local datacenter for the client connection.
   * @param cassandraEndPoints the Cassandra endpoints, only used when the database type is
   *     CASSANDRA
   * @param cassandraPort the Cassandra port, only used when the database type is CASSANDRA
   * @param schemaChangeListenerSupplier an optional supplier called to get a schema change listener
   *     for each new session created
   */
  CqlSessionFactory(
      String applicationName,
      String localDatacenter,
      List<String> cassandraEndPoints,
      Integer cassandraPort,
      Supplier<SchemaChangeListener> schemaChangeListenerSupplier) {
    this(
        applicationName,
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
    this.localDatacenter =
        Objects.requireNonNull(localDatacenter, "localDatacenter must not be null");

    this.schemaChangeListenerSupplier = schemaChangeListenerSupplier;
    this.sessionBuilderSupplier =
        Objects.requireNonNull(sessionBuilderSupplier, "sessionBuilderSupplier must not be null");

    // these never change, so we can cache
    // we cannot test if we need these to be provided until we create the session, because we do not
    // know the DB type until we know the tenant.
    contactPoints =
        cassandraEndPoints != null
            ? cassandraEndPoints.stream()
                .map(host -> new InetSocketAddress(host, cassandraPort))
                .toList()
            : List.of();
  }

  @Override
  public CompletionStage<CqlSession> apply(Tenant tenant, CqlCredentials credentials) {
    Objects.requireNonNull(credentials, "credentials must not be null");

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Creating CQL Session tenant={}, credentials={}", tenant, credentials);
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

    if (builder instanceof TenantAwareCqlSessionBuilder tenantAwareBuilder) {
      tenantAwareBuilder.withTenant(tenant);
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
    if (tenant.databaseType() == DatabaseType.CASSANDRA) {
      if (contactPoints.isEmpty()) {
        throw new IllegalStateException(
            "Database type is %s but contactPoints is empty.".formatted(tenant.databaseType()));
      }
      builder = builder.addContactPoints(contactPoints);
    }

    // Add optimized CqlVector codec (see [data-api#1775])
    builder = builder.addTypeCodecs(SubtypeOnlyFloatVectorToArrayCodec.instance());

    // when we are handling the result of the buildAsync() we need the tenant passed along.
    // simple recompose the functions so they match signature from the framework
    BiFunction<CqlSession, Throwable, CqlSession> partialUnwrapBuildException =
        (session, throwable) -> unwrapBuildException(tenant, session, throwable);
    Function<CqlSession, CompletionStage<CqlSession>> partialValidateSession =
        (session) -> validateSession(tenant, session);

    return builder
        .buildAsync()
        .handle(partialUnwrapBuildException)
        .thenCompose(partialValidateSession);
  }

  /**
   * Process any throwable that was thrown from buildAsync(), pass through the session if no error.
   */
  private static CqlSession unwrapBuildException(
      Tenant tenant, CqlSession cqlSession, Throwable throwable) {

    if (throwable == null) {
      LOGGER.debug(
          "unwrapBuildException() - throwable null. tenant={}, cqlSession={}", tenant, cqlSession);

      return cqlSession;
    }

    // there was an error starting the session, often a token is invalid
    // When the driver throws it's error passes through CompletionStage's and so is
    // wrapped in CompletionException.
    // Sanity check - only get cause in special case above
    var toHandle =
        (throwable instanceof CompletionException && throwable.getCause() != null)
            ? throwable.getCause()
            : throwable;
    LOGGER.debug(
        "unwrapBuildException() - throwable not null. tenant={}, throwable={}, toHandle={}",
        tenant,
        throwable.toString(),
        toHandle.toString());

    var err =
        new DatabaseDriverExceptionHandler(new DatabaseSchemaObject(tenant)).maybeHandle(toHandle);
    LOGGER.debug(
        "unwrapBuildException() - throwable not null. tenant={}, APIException={}",
        tenant,
        err.toString());
    throw err;
  }

  /**
   * Validate that the session returned from the driver has metadata, close the session and error if
   * it is missing. Otherwise, session is good to go.
   *
   * <p>Background: Driver auto reads the metadata, unless disabled, and the API must have metadata
   * so we can check the keyspace + collection/table exist. The metadata read can fail if the token
   * / user+password somehow cannot read the schema tables or if some coordinators do and some do
   * not validate the credentials. In Java Driver see
   * CassandraSchemaQueries.executeOnAdminExecutor() and DefaultSession.initialSchemaRefresh()
   */
  private static CompletionStage<CqlSession> validateSession(Tenant tenant, CqlSession cqlSession) {

    var keyspaces =
        String.join(
            ", ",
            cqlSession.getMetadata().getKeyspaces().keySet().stream()
                .map(Object::toString)
                .toList());
    LOGGER.debug("validateSession() - keyspaces. tenant={}, keyspaces=[{}]", tenant, keyspaces);

    if (cqlSession.getMetadata().getKeyspace("system").isPresent()) {
      return CompletableFuture.completedStage(cqlSession);
    }

    LOGGER.error(
        "validateSession() - system ks not found. tenant={}, keyspaces=[{}]", tenant, keyspaces);
    // NOTE: while throwing the error will prevent the session from getting into the
    // CqlSessionCache we use ExceptionFlags.UNRELIABLE_DB_SESSION tells the CommandProcessor we
    // want to evict the session when from cache when the request is over as belt and braces
    return cqlSession
        .closeAsync()
        .handle(
            (ignored, closeError) -> {
              if (closeError != null) {
                LOGGER.error(
                    "validateSession() - error closing session when metadata not read, tenant={}",
                    tenant,
                    closeError);
              }
              LOGGER.error(
                  "validateSession() - throwing FAILED_TO_READ_METADATA. tenant={}, keyspaces=[{}]",
                  tenant,
                  keyspaces);
              // this is the real error we want to get back to the user
              throw DatabaseException.Code.FAILED_TO_READ_METADATA.get(
                  EnumSet.of(ExceptionFlags.UNRELIABLE_DB_SESSION));
            });
  }
}
