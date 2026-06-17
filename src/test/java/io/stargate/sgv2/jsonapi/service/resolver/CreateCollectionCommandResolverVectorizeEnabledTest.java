package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.util.profiles.EnabledVectorizeProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests how the {@link CreateCollectionCommandResolver} handles inputs and the operation it
 * creates.
 *
 * <p><b>NOTE:</b> subclassed atleast by {@link CreateCollectionCommandResolverVectorizeDisabledTest} to
 * change the vectorize enabled setting
 */
@QuarkusTest
@TestProfile(EnabledVectorizeProfile.class)
class CreateCollectionCommandResolverVectorizeEnabledTest
    extends CreateCollectionCommandResolverTestBase {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(CreateCollectionCommandResolverVectorizeEnabledTest.class);

  @Test
  public void successWithVectorize() {
    var operation =
        assertResolver(
            "successWithVector()",
            """
                        {
                            "createCollection": {
                                "name": "${collection}",
                                "options": {
                                    "vector": {
                                        "metric": "cosine",
                                        "dimension": 768,
                                        "service": {
                                            "provider": "azureOpenAI",
                                            "modelName": "text-embedding-3-small",
                                            "parameters": {
                                                "resourceName": "testResourceName",
                                                "deploymentId": "testResourceName"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        """);

    // NOTE: source model of null turns into DEFAULT
    // aaron 15 june 26 - not sure why but we need the name of the source model must be the CQL name
    // cql cares about capitals, we dont when processing this normally
    var expectedVectorDesc =
        new CreateCollectionCommand.Options.VectorSearchDesc(
            768,
            "cosine",
            EmbeddingSourceModel.DEFAULT.cqlName(),
            new VectorizeConfig(
                "azureOpenAI",
                "text-embedding-3-small",
                null,
                Map.of("resourceName", "testResourceName", "deploymentId", "testResourceName")));

    assertThat(operation.vectorDesc()).isEqualTo(expectedVectorDesc);
  }
}
