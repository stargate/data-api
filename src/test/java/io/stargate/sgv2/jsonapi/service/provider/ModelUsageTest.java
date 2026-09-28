package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import org.junit.jupiter.api.Test;

class ModelUsageTest {

  @Test
  void rejectsGatewayUsageForAnotherTenant() {
    var tenant =
        Tenant.create(DatabaseType.ASTRA, "12345678-1234-1234-1234-123456789abc", "eu-central-1");
    var gatewayUsage =
        EmbeddingGateway.ModelUsage.newBuilder()
            .setModelProvider(ModelProvider.NVIDIA.apiName())
            .setModelType(EmbeddingGateway.ModelUsage.ModelType.RERANKING)
            .setModelName("test-model")
            .setTenantId("87654321-4321-4321-4321-cba987654321")
            .build();

    assertThatThrownBy(() -> ModelUsage.fromEmbeddingGateway(gatewayUsage, tenant))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Gateway model usage tenant does not match request tenant");
  }
}
