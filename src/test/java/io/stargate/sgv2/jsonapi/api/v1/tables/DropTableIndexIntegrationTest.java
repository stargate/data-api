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
class DropTableIndexIntegrationTest extends AbstractTableIntegrationTestBase {

  String simpleTableName = "simpleTableForDropIndexTest";

  @BeforeAll
  public final void createSimpleTable() {
    var json =
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
    assertNamespaceCommand(keyspaceName).postCreateTable(json).wasSuccessful();
  }

  @Nested
  @Order(1)
  class DropIndexSuccess {

    @Test
    @Order(1)
    public void dropIndex() {

      assertTableCommand(keyspaceName, simpleTableName)
          .templated()
          .createIndex("age_idx", "age")
          .wasSuccessful();

      assertTableCommand(keyspaceName, simpleTableName)
          .templated()
          .dropIndex("age_idx")
          .wasSuccessful();
    }
  }
}
