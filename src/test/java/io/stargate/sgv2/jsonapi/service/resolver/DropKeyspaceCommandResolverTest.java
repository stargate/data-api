package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.DropKeyspaceOperation;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class DropKeyspaceCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject DropNamespaceCommandResolver resolver;

  CommandContext<DatabaseSchemaObject> commandContext = TestConstants.databaseContext();

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
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              DropKeyspaceOperation.class,
              op -> assertThat(op.name()).isEqualTo("red_star_belgrade"));
    }
  }
}
