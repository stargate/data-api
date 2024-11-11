package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class ListIndexesIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final String TABLE = "person";
  private static final String createIndex =
      """
              {
                "name": "name_idx",
                "definition": {
                  "column": "name",
                  "options": {
                    "ascii": true,
                    "caseSensitive": false,
                    "normalize": true
                  }
                }
              }
              """;

  String createWithoutOptions =
      """
                  {
                    "name": "city_idx",
                    "definition": {
                      "column": "city"
                    }
                  }
                  """;

  String createWithoutOptionsExpected =
      """
                  {
                    "name": "city_idx",
                    "definition": {
                      "column": "city",
                      "options": {
                        "ascii": false,
                        "caseSensitive": true,
                        "normalize": false
                      }
                    }
                  }
                  """;

  String createVectorIndex =
      """
                {
                 "name": "content_idx",
                 "definition": {
                   "column": "content",
                   "options": {
                     "metric": "cosine",
                     "sourceModel": "openai-v3-small"
                   }
                 }
               }
              """;

  @BeforeAll
  public final void createDefaultTablesAndIndexes() {
    String tableData =
        """
            {
               "name": "%s",
               "definition": {
                   "columns": {
                       "id": "text",
                       "age": "int",
                       "name": "text",
                       "city": "text",
                       "content": {
                           "type": "vector",
                           "dimension": 1024
                       }
                   },
                   "primaryKey": "id"
               }
           }
        """;
    assertNamespaceCommand(keyspaceName)
        .postCreateTable(tableData.formatted(TABLE))
        .wasSuccessful();

    assertTableCommand(keyspaceName, TABLE).postCreateIndex(createIndex).wasSuccessful();

    assertTableCommand(keyspaceName, TABLE).postCreateIndex(createWithoutOptions).wasSuccessful();

    assertTableCommand(keyspaceName, TABLE)
        .postCreateVectorIndex(createVectorIndex)
        .wasSuccessful();
  }

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class ListIndexes {

    @Test
    @Order(1)
    public void listIndexesOnly() {

      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .listIndexes(false)
          .wasSuccessful()
          .hasIndexes("city_idx", "name_idx", "content_idx");
    }

    @Test
    @Order(2)
    public void listIndexessWithDefinition() {

      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .listIndexes(true)
          .wasSuccessful()
          // Validate that status.indexes has all indexes for the table
          .body("status.indexes", hasSize(3))
          // Validate index without options
          .body(
              "status.indexes",
              containsInAnyOrder( // Validate that the indexes are in any order
                  jsonEquals(createIndex),
                  jsonEquals(createWithoutOptionsExpected),
                  jsonEquals(createVectorIndex)));
    }
  }
}
