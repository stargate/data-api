package io.stargate.sgv2.jsonapi.api.v1.tableIntegrationTest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.AbstractNamespaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.NamespaceResource;

/** Abstract class for all table int tests that needs a collection to execute tests in. */
public class AbstractTableIntegrationTestBase extends AbstractNamespaceIntegrationTestBase {

  protected void createTable(String tableName, String createTableJSON) {
    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(createTableJSON.formatted().formatted(tableName))
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200);
  }

  protected void createTable(String createTableJSON) {
    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(createTableJSON)
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("status.ok", is(1));
  }

  protected void deleteAllRowsFromTable(String tableName) {
    String json =
        """
                {
                  "deleteMany": {
                  }
                }
                """;

    while (true) {
      Boolean moreData =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(json)
              .when()
              .post(CollectionResource.BASE_PATH, namespaceName, tableName)
              .then()
              .statusCode(200)
              .body("errors", is(nullValue()))
              .extract()
              .path("status.moreData");

      if (!Boolean.TRUE.equals(moreData)) {
        break;
      }
    }
  }
}
