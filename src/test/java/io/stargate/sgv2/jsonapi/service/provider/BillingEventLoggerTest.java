package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import org.junit.jupiter.api.Test;

class BillingEventLoggerTest {

  private ModelUsage createAstraModelUsage() {
    Tenant tenant = Tenant.create(DatabaseType.ASTRA, "db-uuid-1234");
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "test-model",
        tenant,
        ModelInputType.INDEX,
        100,
        500,
        0,
        0,
        1000L);
  }

  private ModelUsage createCassandraModelUsage() {
    Tenant tenant = Tenant.create(DatabaseType.CASSANDRA, null);
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "test-model",
        tenant,
        ModelInputType.INDEX,
        100,
        500,
        0,
        0,
        1000L);
  }

  @Test
  void logBillingEvent_nonAstraTenant_doesNothing() {
    // should not throw, method returns early for non-ASTRA
    BillingEventLogger.logBillingEvent(createCassandraModelUsage());
  }

  @Test
  void logBillingEvent_astraTenant_logsSuccessfully() {
    // should not throw
    BillingEventLogger.logBillingEvent(createAstraModelUsage());
  }
}
