package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindRowsIntegrationTest extends AbstractCollectionIntegrationTestBase {
  static final String NAMESPACE_NAME = "system";

  static final String COLLECTION_NAME = "local";

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class HappyPath {
    @Test
    @Order(-1)
    public void setUp() {
      ; // nothing to set up yet
    }

    @Test
    public void wrongNamespace() {
      String json =
          """
                    {
                      "findRows": {
                      }
                    }
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, "something_else", COLLECTION_NAME)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[0].message", is("The provided namespace does not exist: something_else"))
          .body("errors[0].errorCode", is("NAMESPACE_DOES_NOT_EXIST"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }
}
