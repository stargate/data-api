package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.*;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

/**
 * Basic testing to see what happens when feature is disabled (other tests will have feature
 * enabled)
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = RerankFeatureDisabledIntegrationTest.TestResource.class,
    // NOTE, restrictToAnnotatedClass has to be true, since most IT use the default DseTestResource,
    // if this set to false, it will impact other integration tests classes.
    // And if it applies to a class with inner test class, set to true will not work, since test
    // resource will not be applied to inner class.
    restrictToAnnotatedClass = true)
public class RerankFeatureDisabledIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  // Need to be able to enable/disable the RERANKING feature
  public static class TestResource extends DseTestResource {
    public TestResource() {}

    @Override
    public String getFeatureFlagReranking() {
      // return empty to leave feature "undefined" (disabled unless per-request header override)
      // ("false" would be "disabled" for all tests, regardless of headers)
      return "";
    }
  }

  private static final String COLLECTION_TO_CREATE = "collection_reranking_feature";

  // By default, FindRerankingProviders should fail
  @Order(1)
  @Test
  public void failFindRerankingProviders() {
    assertGeneralCommand()
        .postFindRerankingProviders()
        .hasSingleApiError(ErrorCodeV1.RERANKING_FEATURE_NOT_ENABLED);
  }

  // But with header override, should succeed
  @Order(2)
  @Test
  public void okFindRerankingProviders() {
    assertGeneralCommand()
        .header(ApiFeature.RERANKING.httpHeaderName(), "true")
        .postFindRerankingProviders()
        .wasSuccessful();
  }

  // By default, collection creation should fail
  @Order(3)
  @Test
  public void failCreateWithoutFeatureEnabled() {

    assertNamespaceCommand(keyspaceName)
        .postCreateCollection(simpleCollectionDef(COLLECTION_TO_CREATE))
        .hasSingleApiError(ErrorCodeV1.RERANKING_FEATURE_NOT_ENABLED);
  }

  // But with header override, should succeed
  @Order(4)
  @Test
  public void okCreateWithFeatureEnabledViaHeader() {
    assertNamespaceCommand(keyspaceName)
        .header(ApiFeature.RERANKING.httpHeaderName(), "true")
        .postCreateCollection(simpleCollectionDef(COLLECTION_TO_CREATE))
        .wasSuccessful();
  }

  // But even with collection, findAndRerank() should fail without Feature enabled
  @Order(5)
  @Test
  public void failFindWithoutFeature() {
    // Temporarily create the command string, refactor when we have templates for FindAndRerank
    var command =
        """
              {
                 "sort": {
                     "$hybrid": {
                         "$vectorize": "i like cheese",
                         "$lexical": "monkeys"
                     }
                 },
                 "options": {
                     "rerankOn": "abc",
                     "limit": 10,
                     "hybridLimits": {
                         "$lexical": 14,
                         "$vector": 14
                     },
                     "includeScores": true,
                     "includeSortVector": false
                 }
                 }
              """;
    assertTableCommand(keyspaceName, COLLECTION_TO_CREATE)
        .postCommand(CommandName.FIND_AND_RERANK, command)
        .hasSingleApiError(ErrorCodeV1.RERANKING_FEATURE_NOT_ENABLED);
  }

  private static String simpleCollectionDef(String collectionName) {
    return
        """
                   {
                          "name": "%s",
                          "options": {
                              "rerank": {
                                  "enabled": true,
                                  "service": {
                                      "provider": "nvidia",
                                      "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"
                                  }
                              }
                          }
                      }
              """
        .formatted(COLLECTION_TO_CREATE);
  }
}
