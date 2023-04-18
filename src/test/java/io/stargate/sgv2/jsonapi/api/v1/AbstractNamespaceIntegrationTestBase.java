package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@TestClassOrder(ClassOrderer.OrderAnnotation.class)
// @TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractNamespaceIntegrationTestBase {

  // namespace automatically created in this test
  protected static final String namespaceName = "ns" + RandomStringUtils.randomAlphanumeric(16);

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  @Order(Integer.MIN_VALUE)
  class Init {

    @Test
    public final void createNamespace() {
      String json =
          """
              {
                "createNamespace": {
                  "name": "%s"
                }
              }
              """
              .formatted(namespaceName);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1))
          .body("errors", is(nullValue()));
    }
  }

  @Nested
  @Order(Integer.MAX_VALUE)
  class CleanUp {

    @Test
    public final void dropNamespace() {
      String json =
          """
              {
                "dropNamespace": {
                  "name": "%s"
                }
              }
              """
              .formatted(namespaceName);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.ok", is(1))
          .body("errors", is(nullValue()));
    }
  }
}
