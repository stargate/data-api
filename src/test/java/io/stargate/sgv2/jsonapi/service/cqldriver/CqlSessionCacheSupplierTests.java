package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCacheSupplier;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Tests for {@link CqlSessionCacheSupplier}. */
public class CqlSessionCacheSupplierTests {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  @Test
  public void testSingleton() {
    // Not a lot to test, just checking it always returns the same instance.

    var dbConfig = mock(OperationsConfig.DatabaseConfig.class);
    when(dbConfig.type()).thenReturn(DatabaseType.ASTRA);
    when(dbConfig.localDatacenter()).thenReturn("datacenter1");
    when(dbConfig.cassandraEndPoints()).thenReturn(List.of());

    when(dbConfig.sessionCacheMaxSize()).thenReturn(100);
    when(dbConfig.sessionCacheTtlSeconds()).thenReturn(60L);
    when(dbConfig.slaSessionCacheTtlSeconds()).thenReturn(120L);

    var operationsConfig = mock(OperationsConfig.class);
    when(operationsConfig.databaseConfig()).thenReturn(dbConfig);
    when(operationsConfig.slaUserAgent())
        .thenReturn(Optional.of(TEST_CONSTANTS.SLA_USER_AGENT_NAME));

    var mockSchemaCache = mock(SchemaObjectCacheSupplier.class);

    var factory =
        new CqlSessionCacheSupplier(
            "testApp", operationsConfig, new SimpleMeterRegistry(), mockSchemaCache);

    var sessionCache1 = factory.get();
    var sessionCache2 = factory.get();

    assertThat(sessionCache1)
        .as("Session cache should be the same instance")
        .isSameAs(sessionCache2);
  }
}
