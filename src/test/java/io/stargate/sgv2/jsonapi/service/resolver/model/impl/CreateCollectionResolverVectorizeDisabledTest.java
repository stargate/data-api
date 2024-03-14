package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CreateCollectionResolverVectorizeDisabledTest {
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
                        "name": "my_collection",
                        "options": {
                            "vector": {
                                "metric": "cosine",
                                "dimension": 768,
                                "service": {
                                    "provider": "vertexai",
                                    "modelName": "textembedding-gecko@003",
                                    "authentication": {
                                        "type": [
                                            "HEADER"
                                        ]
                                    },
                                    "parameters": {
                                        "PROJECT_ID": "test project"
                                    }
                                }
                            }
                        }
                    }
                }
              """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Exception e = catchException(() -> resolver.resolveCommand(commandContext, command));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(ErrorCode.VECTORIZE_FEATURE_NOT_AVAILABLE.getMessage());
    }
  }
}
