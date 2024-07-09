package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DisableVectorizeProfile.class)
public class CreateCollectionResolverVectorizeDisabledTest {
  @Inject ObjectMapper objectMapper;
  @Inject CreateCollectionCommandResolver createCollectionCommandResolver;
  @Inject FindEmbeddingProvidersCommandResolver findEmbeddingProvidersCommandResolver;

  @Nested
  class ResolveCommand {

    @Test
    public void vectorizeSearchDisabled() throws Exception {
      String json =
          """
                {
                    "createCollection": {
                        "name": "my_collection",
                        "options": {
                            "vector": {
                                "metric": "cosine",
                                "dimension": 1024,
                                "service": {
                                    "provider": "nvidia",
                                    "modelName": "NV-Embed-QA"
                                }
                            }
                        }
                    }
                }
              """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Exception e =
          catchException(
              () ->
                  createCollectionCommandResolver.resolveCommand(
                      TestConstants.KEYSPACE_CONTEXT, command));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(ErrorCode.VECTORIZE_FEATURE_NOT_AVAILABLE.getMessage());
    }

    @Test
    public void findEmbeddingProvidersWithVectorizeSearchDisabled() throws Exception {
      // TODO: This test should be moved, these are rests for create collection NOT for
      // findEmbeddingProviders.

      String json =
          """
                  {
                      "findEmbeddingProviders": {}
                  }
                  """;
      FindEmbeddingProvidersCommand command =
          objectMapper.readValue(json, FindEmbeddingProvidersCommand.class);
      Exception e =
          catchException(
              () ->
                  findEmbeddingProvidersCommandResolver.resolveCommand(
                      TestConstants.DATABASE_CONTEXT, command));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(ErrorCode.VECTORIZE_FEATURE_NOT_AVAILABLE.getMessage());
    }
  }
}
