package io.stargate.sgv2.jsonapi.api.v1.tables;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneTableIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nested
  @Order(1)
  class FindOneSuccess {
    @Test
    @Order(1)
    public void findOneSingleStringKey() {
      createTableWithColumns(
          "findOneSingleStringKeyTable",
          Map.of(
              "id",
              Map.of("type", "text"),
              "age",
              Map.of("type", "int"),
              "name",
              Map.of("type", "text")),
          "id");
    }
  }

  @Nested
  @Order(2)
  class FindOneFail {}

  void createTableWithColumns(String tableName, Map<String, Object> columns, Object primaryKeyDef) {
    try {
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
              .formatted(
                  tableName,
                  MAPPER.writeValueAsString(columns),
                  MAPPER.writeValueAsString(primaryKeyDef)));
    } catch (IOException e) { // should never happen
      throw new RuntimeException(e);
    }
  }
}
