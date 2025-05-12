package io.stargate.sgv2.jsonapi.service.cqldriver;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Factory for creating a singleton {@link CQLSessionCache} instance that is configured via CDI.
 *
 * <p>We use this factory so the cache itself is not a CDI bean. For one so it does not have all
 * that extra overhead, and because there construction is a bit more complicated.
 */
@ApplicationScoped
public class CqlSessionCacheSupplier implements Supplier<CQLSessionCache> {

  private final CQLSessionCache singleton;

  @Inject
  public CqlSessionCacheSupplier(
      @ConfigProperty(name = "quarkus.application.name") String applicationName,
      OperationsConfig operationsConfig,
      MeterRegistry meterRegistry,
      SchemaCache schemaCache) {

    Objects.requireNonNull(applicationName, "applicationName must not be null");
    Objects.requireNonNull(operationsConfig, "operationsConfig must not be null");
    Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    Objects.requireNonNull(schemaCache, "schemaCache must not be null");

    var dbConfig = operationsConfig.databaseConfig();

    var credentialsFactory =
        new CqlCredentialsFactory(
            dbConfig.type(),
            dbConfig.fixedToken().orElse(null),
            dbConfig.userName(),
            dbConfig.password());

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
            operationsConfig.slaUserAgent().orElse(null),
            Duration.ofSeconds(dbConfig.slaSessionCacheTtlSeconds()),
            credentialsFactory,
            sessionFactory,
            meterRegistry,
            List.of(schemaCache.getDeactivatedTenantConsumer()));
  }

  /** Gets the singleton instance of the {@link CQLSessionCache}. */
  @Override
  public CQLSessionCache get() {
    return singleton;
  }
}
