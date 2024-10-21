package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiResponseValidator;
import java.io.IOException;
import java.util.Map;

/** Abstract class for all table int tests that needs a collection to execute tests in. */
public class AbstractTableIntegrationTestBase extends AbstractKeyspaceIntegrationTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected DataApiResponseValidator createTableWithColumns(
      String tableName, Map<String, Object> columns, Object primaryKeyDef) {
    return createTable(
            """
            {
                "name": "%s",
                "definition": {
                    "columns": %s,
                    "primaryKey": %s
                }
            }
            """
            .formatted(tableName, asJSON(columns), asJSON(primaryKeyDef)));
  }

  protected DataApiResponseValidator createTable(String tableDefAsJSON) {
    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
        .postCreateTable(tableDefAsJSON)
        .hasNoErrors()
        .body("status.ok", is(1));
  }

  protected DataApiResponseValidator listTables(String tableDefAsJSON) {
    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
        .postListTables(tableDefAsJSON)
        .hasNoErrors()
        .body("status.tables", notNullValue());
  }

  protected DataApiResponseValidator dropTable(String tableDefAsJSON) {
    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
        .postDropTable(tableDefAsJSON)
        .hasNoErrors()
        .body("status.ok", is(1));
  }

  protected DataApiResponseValidator dropIndex(String tableDefAsJSON) {
    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
        .postDropIndex(tableDefAsJSON)
        .hasNoErrors()
        .body("status.ok", is(1));
  }

  protected DataApiResponseValidator createTableErrorValidation(
      String tableDefAsJSON, String errorCode, String message) {
    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
        .postCreateTable(tableDefAsJSON)
        .body("errors[0].errorCode", is(errorCode))
        .body("errors[0].message", containsString(message));
  }

  protected DataApiResponseValidator deleteTable(String tableName) {
    // 09-Sep-2024, tatu: No separate "deleteTable" command, so use "deleteCollection":
    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
        .postCommand("deleteCollection", "{\"name\": \"%s\"}".formatted(tableName))
        .hasNoErrors()
        .body("status.ok", is(1));
  }

  protected DataApiResponseValidator insertOneInTable(String tableName, String documentJSON) {
    return DataApiCommandSenders.assertTableCommand(keyspaceName, tableName)
        .postInsertOne(documentJSON)
        .hasNoErrors()
        .body("status.insertedIds", hasSize(1));
  }

  protected DataApiResponseValidator createIndex(
      String tableName, String columnName, String indexName) {
    return DataApiCommandSenders.assertTableCommand(keyspaceName, tableName)
        .postCreateIndex(columnName, indexName)
        .hasNoErrors()
        .body("status.ok", is(1));
  }

  protected DataApiResponseValidator createIndex(String tableName, String columnName) {
    String indexName = String.format("%s_%s_index", tableName, columnName);
    return DataApiCommandSenders.assertTableCommand(keyspaceName, tableName)
        .postCreateIndex(columnName, indexName)
        .hasNoErrors()
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
              .post(CollectionResource.BASE_PATH, keyspaceName, tableName)
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
