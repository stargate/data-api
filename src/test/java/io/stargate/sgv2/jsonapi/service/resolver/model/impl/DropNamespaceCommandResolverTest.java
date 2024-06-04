package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DropNamespaceOperation;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class DropNamespaceCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject DropNamespaceCommandResolver resolver;

  @Nested
  class ResolveCommand {

    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "dropNamespace": {
              "name" : "red_star_belgrade"
            }
          }
          """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Operation result = resolver.resolveCommand(null, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              DropNamespaceOperation.class,
              op -> assertThat(op.name()).isEqualTo("red_star_belgrade"));
    }
  }
}
