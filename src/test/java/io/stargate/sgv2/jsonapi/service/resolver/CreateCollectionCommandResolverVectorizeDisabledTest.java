package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.asserts.DataAPIAsserts.assertThatSchemaException;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.util.profiles.DisableVectorizeProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DisableVectorizeProfile.class)
public class CreateCollectionCommandResolverVectorizeDisabledTest
    extends CreateCollectionCommandResolverTestBase {

  @Test
  public void failVectorizeSearchDisabled() {

    var throwable =
        assertResolverThrows(
            "failVectorizeSearchDisabled()",
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
                                """);

    assertThatSchemaException(throwable)
        .as("failVectorizeSearchDisabled()")
        .hasCode(SchemaException.Code.VECTORIZE_FEATURE_NOT_AVAILABLE);
  }
}
