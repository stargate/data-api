package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.FindCollectionsOperation;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCollectionCommandResolverTest {
  @Inject ObjectMapper objectMapper;

  @Inject FindCollectionsCommandResolver resolver;

  @Nested
  class FindCollectionCommandResolveCommand {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.CONTEXT;

    @Test
    public void findCollection() throws Exception {
      String json =
          """
          {
            "findCollections": {

            }
          }
          """;

      FindCollectionsCommand findCommand =
          objectMapper.readValue(json, FindCollectionsCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionsOperation.class,
              findCollection -> {
                assertThat(findCollection.objectMapper()).isEqualTo(objectMapper);
                assertThat(findCollection.commandContext()).isEqualTo(commandContext);
                assertThat(findCollection.explain()).isEqualTo(false);
              });
    }

    @Test
    public void findCollectionWithExplain() throws Exception {
      String json =
          """
                  {
                    "findCollections": {
                      "options" : {
                        "explain" : true
                      }
                    }
                  }
                  """;

      FindCollectionsCommand findCommand =
          objectMapper.readValue(json, FindCollectionsCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, findCommand);

      assertThat(operation)
          .isInstanceOfSatisfying(
              FindCollectionsOperation.class,
              findCollection -> {
                assertThat(findCollection.objectMapper()).isEqualTo(objectMapper);
                assertThat(findCollection.commandContext()).isEqualTo(commandContext);
                assertThat(findCollection.explain()).isEqualTo(true);
              });
    }
  }
}
