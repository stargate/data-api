package io.stargate.sgv3.docsapi.api.v3;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.CqlEnabledIntegrationTestBase;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DatabaseResourceIntegrationTest extends CqlEnabledIntegrationTestBase {

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class PostCommand {

    @Test
    public void happyPath() {
      String json =
          String.format(
              """
                                      {
                                        "createCollection": {
                                          "name": "%s"
                                        }
                                      }
                                      """,
              "col" + RandomStringUtils.randomNumeric(16));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(DatabaseResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    @DisabledIfSystemProperty(
        named = "testing.package.type",
        matches = "native",
        disabledReason =
            "[V2 exception mappers map to ApiError which is not registered for refection](https://github.com/riptano/sgv3-docsapi/issues/8)")
    public void error() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .when()
          .post(DatabaseResource.BASE_PATH, keyspaceId.asInternal())
          .then()
          .statusCode(400);
    }
  }
}
