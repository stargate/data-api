package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropIndexIntegrationTest extends AbstractTableIntegrationTestBase {

  String simpleTableName = "simple_table_drop_index_test";

  @BeforeAll
  public final void createSimpleTable() {
    String tableJson =
            """
                              {
                                      "name": "%s",
                                      "definition": {
                                          "columns": {
                                              "id": {
                                                  "type": "text"
                                              },
                                              "age": {
                                                  "type": "int"
                                              },
                                              "name": {
                                                  "type": "text"
                                              }
                                          },
                                          "primaryKey": "id"
                                      }
                            }
                        """
            .formatted(simpleTableName);
    createTable(tableJson);
    createIndex(simpleTableName, "age", "age_idx");
    createIndex(simpleTableName, "name", "name_idx");
  }

  @Nested
  @Order(1)
  class DropIndexSuccess {
    @Test
    public void dropIndexWithoutOption() {
      String dropIndexJson =
          """
        {
            "indexName": "age_idx"
        }
        """;
      dropIndex(dropIndexJson);
    }

    @Test
    public void dropIndexWithOption() {
      String dropIndexJson =
          """
        {
            "indexName": "name_idx",
            "options" : {
                "ifExists": true
            }
        }
        """;

      for (int i = 0; i < 2; i++) {
        dropIndex(dropIndexJson);
      }
    }
  }
}
