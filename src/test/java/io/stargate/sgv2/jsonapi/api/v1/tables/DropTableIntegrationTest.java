package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropTableIntegrationTest extends AbstractTableIntegrationTestBase {

  String simpleTableName = "simple_table_drop_index_test";
  String duplicateTableName = "duplicate_table_drop_index_test";

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
                            """;

    assertNamespaceCommand(keyspaceName)
        .postCreateTable(tableJson.formatted(simpleTableName))
        .wasSuccessful();
    assertNamespaceCommand(keyspaceName)
        .postCreateTable(tableJson.formatted(duplicateTableName))
        .wasSuccessful();
  }

  @Nested
  @Order(1)
  class DropTableSuccess {
    @Test
    public void dropTableWithoutOption() {
      String dropTableJson =
          """
                    {
                        "name" : "%s"
                    }
                    """;
      assertNamespaceCommand(keyspaceName).postDropTable(dropTableJson.formatted(simpleTableName));
    }

    @Test
    public void dropTableWithOption() {
      String dropTableJson =
          """
                    {
                        "name": "%s",
                        "options" : {
                            "ifExists": true
                        }
                    }
                    """;

      for (int i = 0; i < 2; i++) {
        assertNamespaceCommand(keyspaceName)
            .postDropTable(dropTableJson.formatted(duplicateTableName));
      }
    }
  }
}