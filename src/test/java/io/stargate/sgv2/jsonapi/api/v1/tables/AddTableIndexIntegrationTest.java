package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class AddTableIndexIntegrationTest extends AbstractTableIntegrationTestBase {
  String testTableName = "tableForAddIndexTest";

  @BeforeAll
  public final void createSimpleTable() {
    createTableWithColumns(
        testTableName,
        Map.of(
            "id",
            Map.of("type", "text"),
            "age",
            Map.of("type", "int"),
            "vehicleId",
            Map.of("type", "text"),
            "name",
            Map.of("type", "text")),
        "id");
  }

  @Nested
  @Order(1)
  class AddIndexSuccess {

    @Test
    public void addIndexBasic() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "addIndex",
              """
                  {
                          "column": "age",
                          "indexName": "age_index"
                  }
                  """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void addIndexCaseSensitive() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "addIndex",
              """
                          {
                                  "column": "vehicleId",
                                  "indexName": "vehicleId_idx"
                          }
                          """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class AddIndexFailure {
    @Test
    public void tryAddIndexMissingColumn() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "addIndex",
              """
                      {
                              "column": "city",
                              "indexName": "city_index"
                      }
                      """)
          .hasSingleApiError(ErrorCodeV1.INVALID_QUERY, "Undefined column name city");
    }
  }
}
