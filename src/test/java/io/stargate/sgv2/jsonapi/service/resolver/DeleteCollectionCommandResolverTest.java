package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionCollectionOperation;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class DeleteCollectionCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject DeleteCollectionCommandResolver resolver;

  private TestConstants testConstants = new TestConstants();

  CommandContext<KeyspaceSchemaObject> commandContext;

  @BeforeEach
  public void beforeEach() {
    commandContext = testConstants.keyspaceContext();
  }

  @Nested
  class ResolveCommand {

    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "deleteCollection": {
              "name" : "my_collection"
            }
          }
          """;

      DeleteCollectionCommand command = objectMapper.readValue(json, DeleteCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              DeleteCollectionCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.context()).isEqualTo(commandContext);
              });
    }
  }
}
