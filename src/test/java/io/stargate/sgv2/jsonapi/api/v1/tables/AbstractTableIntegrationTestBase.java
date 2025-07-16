package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.hamcrest.Matchers.hasSize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiResponseValidator;
import java.io.IOException;

/** Abstract class for all table int tests that needs a table to execute tests in. */
public class AbstractTableIntegrationTestBase extends AbstractKeyspaceIntegrationTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  String removeNullValues(String doc) {
    ObjectNode newNode = MAPPER.createObjectNode();
    JsonNode oldNode;
    try {
      oldNode = MAPPER.readTree(doc);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse JSON: " + doc, e);
    }
    oldNode
        .fields()
        .forEachRemaining(
            entry -> {
              JsonNode value = entry.getValue();
              if (!value.isNull()) {
                newNode.putIfAbsent(entry.getKey(), value);
              }
            });
    return newNode.toString();
  }

  //  protected DataApiResponseValidator createTableWithColumns(
  //      String tableName, Map<String, Object> columns, Object primaryKeyDef) {
  //    return createTable(
  //            """
  //            {
  //                "name": "%s",
  //                "definition": {
  //                    "columns": %s,
  //                    "primaryKey": %s
  //                }
  //            }
  //            """
  //            .formatted(tableName, asJSON(columns), asJSON(primaryKeyDef)));
  //  }

  //  protected DataApiResponseValidator createTable(String tableDefAsJSON) {
  //    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
  //        .postCreateTable(tableDefAsJSON)
  //        .hasNoErrors()
  //        .body("status.ok", is(1));
  //  }

  //  protected DataApiResponseValidator listTables(String tableDefAsJSON) {
  //    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
  //        .postListTables(tableDefAsJSON)
  //        .hasNoErrors()
  //        .body("status.tables", notNullValue());
  //  }

  //  protected DataApiResponseValidator createTableErrorValidation(
  //      String tableDefAsJSON, String errorCode, String message) {
  //    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
  //        .postCreateTable(tableDefAsJSON)
  //        .body("errors[0].errorCode", is(errorCode))
  //        .body("errors[0].message", containsString(message));
  //  }

  //  protected DataApiResponseValidator deleteTable(String tableName) {
  //    return DataApiCommandSenders.assertNamespaceCommand(keyspaceName)
  //        .postDropTable("{\"name\": \"%s\"}".formatted(tableName))
  //        .hasNoErrors()
  //        .body("status.ok", is(1));
  //  }

  protected DataApiResponseValidator insertOneInTable(String tableName, Object docValue) {
    return insertOneInTable(keyspaceName, tableName, docValue);
  }

  protected DataApiResponseValidator insertOneInTable(
      String keyspaceName, String tableName, Object docValue) {
    final String documentJSON = docValue instanceof String str ? str : asJson(docValue);
    return DataApiCommandSenders.assertTableCommand(keyspaceName, tableName)
        .postInsertOne("{\"document\": %s}".formatted(documentJSON))
        .hasNoErrors()
        .body("status.insertedIds", hasSize(1));
  }

  //  protected DataApiResponseValidator createIndex(
  //      String tableName, String columnName, String indexName) {
  //
  //    return DataApiCommandSenders.assertTableCommand(keyspaceName, tableName)
  //        .doCreateIndex(columnName, indexName)
  //        .hasNoErrors()
  //        .body("status.ok", is(1));
  //  }

  //  protected DataApiResponseValidator createIndex(String tableName, String columnName) {
  //    String indexName = String.format("%s_%s_index", tableName, columnName);
  //    return DataApiCommandSenders.assertTableCommand(keyspaceName, tableName)
  //        .doCreateIndex(indexName, columnName)
  //        .hasNoErrors()
  //        .body("status.ok", is(1));
  //  }

  //  protected void deleteAllRowsFromTable(String tableName) {
  //    String json =
  //        """
  //                {
  //                  "deleteMany": {
  //                  }
  //                }
  //                """;
  //
  //    while (true) {
  //      Boolean moreData =
  //          given()
  //              .headers(getHeaders())
  //              .contentType(ContentType.JSON)
  //              .body(json)
  //              .when()
  //              .post(CollectionResource.BASE_PATH, keyspaceName, tableName)
  //              .then()
  //              .statusCode(200)
  //              .body("errors", is(nullValue()))
  //              .extract()
  //              .path("status.moreData");
  //
  //      if (!Boolean.TRUE.equals(moreData)) {
  //        break;
  //      }
  //    }
  //  }

  protected String asJson(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to convert value to JSON: " + value, e);
    }
  }
}
