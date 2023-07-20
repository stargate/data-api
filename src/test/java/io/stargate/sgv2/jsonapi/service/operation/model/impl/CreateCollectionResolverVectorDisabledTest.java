package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.CreateCollectionCommandResolver;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DisableVectorSearchProfile.class)
public class CreateCollectionResolverVectorDisabledTest {
  @Inject ObjectMapper objectMapper;
  @Inject CreateCollectionCommandResolver resolver;

  @Nested
  class ResolveCommand {

    @Mock CommandContext commandContext;

    @Test
    public void vectorSearchDisabled() throws Exception {
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
      Exception e = catchException(() -> resolver.resolveCommand(commandContext, command));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(ErrorCode.VECTOR_SEARCH_NOT_AVAILABLE.getMessage());
    }
  }
}
