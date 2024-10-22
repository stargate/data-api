package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

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

    assertNamespaceCommand(keyspaceName).postCreateTable(tableJson).wasSuccessful();
    assertTableCommand(keyspaceName, simpleTableName).templated().createIndex("age_idx", "age");
    assertTableCommand(keyspaceName, simpleTableName).templated().createIndex("name_idx", "name");
  }

  @Nested
  @Order(1)
  class DropIndexSuccess {
    @Test
    public void dropIndexWithoutOption() {
      String dropIndexJson =
          """
        {
            "name": "age_idx"
        }
        """;
      assertNamespaceCommand(keyspaceName).postDropIndex(dropIndexJson).wasSuccessful();
    }

    @Test
    public void dropIndexWithOption() {
      String dropIndexJson =
          """
        {
            "name": "name_idx",
            "options" : {
                "ifExists": true
            }
        }
        """;

      for (int i = 0; i < 2; i++) {
        assertNamespaceCommand(keyspaceName)
            .postDropIndex(dropIndexJson)
            .wasSuccessful(); // should not fail
      }
    }
  }
}
