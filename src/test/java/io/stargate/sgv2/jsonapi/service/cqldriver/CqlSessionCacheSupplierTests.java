package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link CqlSessionCacheSupplier}. */
public class CqlSessionCacheSupplierTests {

  @Test
  public void testSingleton() {
    // Not a lot to test, just checking it always returns the same instance.

    var dbConfig = mock(OperationsConfig.DatabaseConfig.class);
    when(dbConfig.type()).thenReturn(DatabaseType.ASTRA);
    when(dbConfig.localDatacenter()).thenReturn("datacenter1");
    when(dbConfig.cassandraEndPoints()).thenReturn(List.of());

    var operationsConfig = mock(OperationsConfig.class);
    when(operationsConfig.databaseConfig()).thenReturn(dbConfig);

    var schemaCache = mock(SchemaCache.class);
    when(schemaCache.getSchemaChangeListener()).thenReturn(mock(SchemaChangeListener.class));
    when(schemaCache.getDeactivatedTenantConsumer())
        .thenReturn(mock(CQLSessionCache.DeactivatedTenantConsumer.class));

    var factory =
        new CqlSessionCacheSupplier(
            "testApp", operationsConfig, new SimpleMeterRegistry(), schemaCache);

    var sessionCache1 = factory.get();
    var sessionCache2 = factory.get();

    assertThat(sessionCache1)
        .as("Session cache should be the same instance")
        .isSameAs(sessionCache2);
  }
}
