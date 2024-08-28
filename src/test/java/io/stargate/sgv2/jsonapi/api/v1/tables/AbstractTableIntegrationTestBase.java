package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.AbstractNamespaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.NamespaceResource;
import java.io.IOException;
import java.util.Map;

/** Abstract class for all table int tests that needs a collection to execute tests in. */
public class AbstractTableIntegrationTestBase extends AbstractNamespaceIntegrationTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected void createTableWithColumns(
      String tableName, Map<String, Object> columns, Object primaryKeyDef) {
    createTable(
            """
                  {
                      "createTable": {
                          "name": "%s",
                          "definition": {
                              "columns": %s,
                              "primaryKey": %s
                          }
                      }
                  }
            """
            .formatted(tableName, asJSON(columns), asJSON(primaryKeyDef)));
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

  protected void insertOneInTable(String tableName, String documentJSON) {
    final String requestJSON =
            """
            {
              "insertOne": {
                "document": %s
              }
            }
            """
            .formatted(documentJSON);
    given()
        .port(getTestPort())
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(requestJSON)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, tableName)
        .then()
        .statusCode(200)
        .body("status.insertedIds", hasSize(1))
        .body("data", is(nullValue()))
        .body("errors", is(nullValue()));
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

  protected static String asJSON(Object ob) {
    try {
      return MAPPER.writeValueAsString(ob);
    } catch (IOException e) { // should never happen
      throw new RuntimeException(e);
    }
  }
}
