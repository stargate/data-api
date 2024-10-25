package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
    String createIndex =
        """
            {
              "name": "name_idx",
              "definition": {
                "column": "name",
                "options": {
                  "normalize": true,
                  "ascii": true
                }
              }
            }
            """;
    assertTableCommand(keyspaceName, TABLE).postCreateIndex(createIndex).wasSuccessful();
    String createWithoutOptions =
        """
                {
                  "name": "city_idx",
                  "definition": {
                    "column": "city"
                  }
                }
                """;
    assertTableCommand(keyspaceName, TABLE).postCreateIndex(createWithoutOptions).wasSuccessful();
    String createVectorIndex =
        """
              {
               "name": "content_idx",
               "definition": {
                 "column": "content",
                 "options": {
                   "metric": "cosine",
                   "sourceModel": "openai_v3_small"
                 }
               }
             }
            """;
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
          // Validate that status.indexes is not null and has all indexes for the table
          .body("status.indexes", notNullValue())
          .body("status.indexes", hasSize(3))
          .body("status.indexes", notNullValue())
          .body("status.indexes", hasSize(3))
          // Validate index without options
          .body("status.indexes[0].name", equalTo("city_idx"))
          .body("status.indexes[0].definition.column", equalTo("city"))
          .body("status.indexes[0].definition.options", nullValue())

          // validate index with options
          .body("status.indexes[1].name", equalTo("name_idx"))
          .body("status.indexes[1].definition.column", equalTo("name"))
          .body("status.indexes[1].definition.options.normalize", equalTo(true))
          .body("status.indexes[1].definition.options.ascii", equalTo(true))
          .body("status.indexes[1].definition.options.caseSensitive", nullValue())

          // Validate vector index
          .body("status.indexes[2].name", equalTo("content_idx"))
          .body("status.indexes[2].definition.column", equalTo("content"))
          .body("status.indexes[2].definition.options.metric", equalTo("cosine"))
          .body("status.indexes[2].definition.options.sourceModel", equalTo("openai_v3_small"));
    }
  }
}
