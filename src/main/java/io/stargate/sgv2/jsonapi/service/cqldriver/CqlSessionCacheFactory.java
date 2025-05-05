package io.stargate.sgv2.jsonapi.service.cqldriver;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Factory for creating a singleton {@link CQLSessionCache} instance that is configured via CDI.
 *
 * <p>We use this factory so the cache itself is not a CDI bean. For one so it does not have all
 * that extra overhead, and because there construction is a bit more complicated.
 */
@ApplicationScoped
public class CqlSessionCacheFactory {

  private CQLSessionCache singleton = null;

  private final String applicationName;
  private final OperationsConfig operationsConfig;
  private final MeterRegistry meterRegistry;
  private final SchemaCache schemaCache;

  @Inject
  public CqlSessionCacheFactory(
      @ConfigProperty(name = "quarkus.application.name") String applicationName,
      OperationsConfig operationsConfig,
      MeterRegistry meterRegistry,
      SchemaCache schemaCache) {

    this.applicationName =
        Objects.requireNonNull(applicationName, "applicationName must not be null");
    this.operationsConfig =
        Objects.requireNonNull(operationsConfig, "operationsConfig must not be null");
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    this.schemaCache = Objects.requireNonNull(schemaCache, "schemaCache must not be null");
  }

  /**
   * Gets the singleton instance of the {@link CQLSessionCache}.
   *
   * <p>Is thread safe, will only create the cache once, and will return the same instance on all
   * calls.
   *
   * @return the singleton instance of the {@link CQLSessionCache}
   */
  public CQLSessionCache create() {

    if (singleton != null) {
      return singleton;
    }
    createCache();

    if (singleton == null) {
      // sanity check
      throw new IllegalStateException("Could not create CQLSessionCache");
    }
    return singleton;
  }

  private void createCache() {
    synchronized (this) {
      if (singleton != null) {
        return;
      }

      var dbConfig = operationsConfig.databaseConfig();

      var credentialsFactory =
          new CqlCredentials.CqlCredentialsFactory(
              dbConfig.fixedToken().orElse(null),
              dbConfig.userName(),
              dbConfig.password(),
              dbConfig.type());

      var sessionFactory =
          new CqlSessionFactory(
              applicationName,
              dbConfig.type(),
              dbConfig.localDatacenter(),
              dbConfig.cassandraEndPoints(),
              dbConfig.cassandraPort(),
              List.of(schemaCache.getSchemaChangeListener()));

      singleton =
          new CQLSessionCache(
              dbConfig.type(),
              Duration.ofSeconds(dbConfig.sessionCacheTtlSeconds()),
              dbConfig.sessionCacheMaxSize(),
              credentialsFactory,
              sessionFactory,
              meterRegistry,
              List.of(schemaCache.getDeactivatedTenantConsumer()));
    }
  }
}
