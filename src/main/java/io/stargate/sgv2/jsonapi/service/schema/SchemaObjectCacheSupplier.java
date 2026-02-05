package io.stargate.sgv2.jsonapi.service.schema;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.function.Supplier;

@ApplicationScoped
public class SchemaObjectCacheSupplier implements Supplier<SchemaObjectCache> {

  private final SchemaObjectCache singleton;

  @Inject
  public SchemaObjectCacheSupplier(
      CqlSessionCacheSupplier cqlSessionCacheSupplier,
      OperationsConfig operationsConfig,
      MeterRegistry meterRegistry) {

    var dbConfig = operationsConfig.databaseConfig();

    var factory = new SchemaObjectFactory(cqlSessionCacheSupplier);

    this.singleton =
        new SchemaObjectCache(
            dbConfig.sessionCacheMaxSize(),
            Duration.ofSeconds(dbConfig.sessionCacheTtlSeconds()),
            operationsConfig.slaUserAgent().map(UserAgent::new).orElse(null),
            Duration.ofSeconds(dbConfig.slaSessionCacheTtlSeconds()),
            factory,
            meterRegistry);
  }

  @Override
  public SchemaObjectCache get() {
    return singleton;
  }
}
