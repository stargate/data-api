package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateEmbeddingServiceCommand;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateEmbeddingServiceOperation;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(CreateEmbeddingServiceCommandResolverTest.MemoryBasedOverrideProfile.class)
class CreateEmbeddingServiceCommandResolverTest {

  @Inject Instance<EmbeddingServiceConfigStore> embeddingServiceConfigStore;
  @Inject ObjectMapper objectMapper;

  @Nested
  class ResolveCommand {

    @Test
    public void noOptions() throws Exception {
      String json =
          """
                  {
                    "createEmbeddingService": {
                      "name": "my_service",
                      "apiProvider" : "openai",
                      "apiKey" : "test token",
                      "baseUrl" : "https://api.openai.com/v1/"
                    }
                  }
                  """;
      CreateEmbeddingServiceCommand command =
          objectMapper.readValue(json, CreateEmbeddingServiceCommand.class);
      CreateEmbeddingServiceCommandResolver resolver =
          new CreateEmbeddingServiceCommandResolver(embeddingServiceConfigStore, null);
      Operation result = resolver.resolveCommand(null, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateEmbeddingServiceOperation.class,
              op -> {
                assertThat(op.serviceName()).isEqualTo("my_service");
                assertThat(op.providerName()).isEqualTo("openai");
                assertThat(op.apiKey()).isEqualTo("test token");
                assertThat(op.baseUrl()).isEqualTo("https://api.openai.com/v1/");
                assertThat(op.tenant()).isEqualTo(Optional.empty());
              });
    }
  }

  public static class MemoryBasedOverrideProfile implements QuarkusTestProfile {
    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.jsonapi.embedding.config.store", "in-memory")
          .build();
    }
  }
}
