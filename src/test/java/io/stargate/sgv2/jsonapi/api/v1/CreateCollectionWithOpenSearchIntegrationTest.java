package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.testresource.StargateTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class CreateCollectionWithOpenSearchIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Test
  void createsCollectionWithOpenSearch() {
    Assumptions.assumeTrue(StargateTestResource.isHcd());

    String collectionName = "coll_open_search" + RandomStringUtils.insecure().nextNumeric(16);
    givenHeadersPostJsonThenOkNoErrors(
                """
            {
              "createCollection": {
                "name": "%s",
                "options": {
                  "openSearch": {
                    "enabled": true,
                    "mappings": {
                      "title": { "type": "text" },
                      "published": { "type": "boolean" }
                    }
                  }
                }
              }
            }
            """
                .formatted(collectionName))
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));

    assertThat(
            executeCqlStatement(
                    """
                ALTER TABLE %s.%s WITH comment = '{"collection":{"name":"%s","schema_version":2,"options":{"defaultId":{"type":""},"lexical":{"enabled":true,"analyzer":"standard"},"rerank":{"enabled":true,"service":{"provider":"nvidia","modelName":"nvidia/llama-3.2-nv-rerankqa-1b-v2","authentication":null,"parameters":null}}}}}';
                """
                    .formatted(keyspaceName, collectionName, collectionName)))
        .isTrue();

    givenHeadersPostJsonThenOkNoErrors(
            """
            {
              "findCollections": {
                "options": {
                  "explain": true
                }
              }
            }
            """)
        .body("$", responseIsDDLSuccess())
        .body("status.collections[0].name", is(collectionName))
        .body("status.collections[0].options.openSearch.enabled", is(true))
        .body("status.collections[0].options.openSearch.mappings.title.type", is("text"))
        .body("status.collections[0].options.openSearch.mappings.published.type", is("boolean"));

    givenHeadersPostJsonThenOkNoErrors(
                """
            {
              "deleteCollection": {
                "name": "%s"
              }
            }
            """
                .formatted(collectionName))
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
  }
}
