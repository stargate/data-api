package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateCollectionCommandResolverTest {

  @Inject ObjectMapper objectMapper;
  @Inject CreateCollectionCommandResolver resolver;

  @Nested
  class ResolveCommand {

    @Mock CommandContext commandContext;

    @Test
    public void happyPath() throws Exception {
      String json =
          """
              {
                "createCollection": {
                  "name" : "my_collection"
                }
              }
              """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(false);
                assertThat(op.vectorSize()).isEqualTo(0);
                assertThat(op.vectorFunction()).isNull();
              });
    }

    @Test
    public void happyPathVectorSearch() throws Exception {
      String json =
          """
            {
              "createCollection": {
                "name" : "my_collection",
                "options": {
                  "vector": {
                    "size": 4,
                    "function": "cosine"
                  }
                }
              }
            }
            """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(true);
                assertThat(op.vectorSize()).isEqualTo(4);
                assertThat(op.vectorFunction()).isEqualTo("cosine");
              });
    }

    @Test
    public void happyPathVectorizeSearch() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name" : "my_collection",
              "options": {
                "vector": {
                  "size": 4,
                  "function": "cosine"
                },
                "vectorize": {
                  "service" : "openai",
                  "options" : {
                    "modelName": "text-embedding-ada-002"
                  }
                }
              }
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(true);
                assertThat(op.vectorSize()).isEqualTo(4);
                assertThat(op.vectorFunction()).isEqualTo("cosine");
                assertThat(op.vectorize())
                    .isEqualTo(
                        "{\"service\":\"openai\",\"options\":{\"modelName\":\"text-embedding-ada-002\"}}");
              });
    }

    @Test
    public void happyPathVectorSearchDefaultFunction() throws Exception {
      String json =
          """
        {
          "createCollection": {
            "name" : "my_collection",
            "options": {
              "vector": {
                "size": 4
              }
            }
          }
        }
        """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Operation result = resolver.resolveCommand(commandContext, command);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionOperation.class,
              op -> {
                assertThat(op.name()).isEqualTo("my_collection");
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.vectorSearch()).isEqualTo(true);
                assertThat(op.vectorSize()).isEqualTo(4);
                assertThat(op.vectorFunction()).isEqualTo("cosine");
              });
    }
  }
}
