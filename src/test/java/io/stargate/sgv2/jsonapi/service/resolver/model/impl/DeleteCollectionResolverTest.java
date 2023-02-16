package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteCollectionOperation;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class DeleteCollectionResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject DeleteCollectionResolver resolver;

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
      CommandContext context = new CommandContext("my_namespace", null);
      Operation result = resolver.resolveCommand(context, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              DeleteCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.context()).isEqualTo(context);
              });
    }
  }
}
