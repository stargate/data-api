package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneTableIntegrationTest extends AbstractTableIntegrationTestBase {
  static final String TABLE_WITH_STRING_ID_AGE_NAME = "findOneSingleStringKeyTable";
  static final String TABLE_WITH_TEXT_COLUMNS = "findOneTextColumnsTable";

  @BeforeAll
  public final void createDefaultTables() {
    createTableWithColumns(
        TABLE_WITH_STRING_ID_AGE_NAME,
        Map.of(
            "id",
            Map.of("type", "text"),
            "age",
            Map.of("type", "int"),
            "name",
            Map.of("type", "text")),
        "id");
    createTableWithColumns(
            TABLE_WITH_TEXT_COLUMNS,
            Map.of(
                    "id",
                    Map.of("type", "int"),
                    "asciiText",
                    Map.of("type", "ascii"),
                    "varcharText",
                    Map.of("type", "text")),
            "id");

  }

  // On-empty tests to be run before ones that populate tables
  @Nested
  @Order(1)
  class FindOneOnEmpty {
    @Test
    public void findOnEmptyNoFilter() {
      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne("{ }")
          .hasNoErrors()
          .hasNoField("data.document");
    }

    @Test
    public void findOnEmptyNonMatchingFilter() {
      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
                                  {
                                    "filter": {
                                        "id": "nosuchkey"
                                    }
                                  }
                              """)
          .hasNoErrors()
          .hasNoField("data.document");
    }
  }

  @Nested
  @Order(2)
  class FindOneSuccess {
    @Test
    @Order(1)
    public void findOneSingleStringKey() {
      // First, insert 2 documents:
      insertOneInTable(
          TABLE_WITH_STRING_ID_AGE_NAME,
          """
                      {
                          "id": "a",
                          "age": 20,
                          "name": "John"
                      }
                      """);
      final String DOC_B_JSON =
          """
                          {
                              "id": "b",
                              "age": 40,
                              "name": "Bob"
                          }
                          """;
      insertOneInTable(TABLE_WITH_STRING_ID_AGE_NAME, DOC_B_JSON);

      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
                          {
                              "filter": {
                                  "id": "b"
                              }
                          }
                      """)
          .hasNoErrors()
          .hasJSONField("data.document", DOC_B_JSON);
    }

    @Test
    @Order(2)
    public void findOneDocUpperCaseKey() {
      final String TABLE_NAME = "findOneDocUpperCaseKey";
      createTableWithColumns(
          TABLE_NAME, Map.of("Id", Map.of("type", "int"), "value", Map.of("type", "text")), "Id");

      // Insert 2 documents:
      insertOneInTable(
          TABLE_NAME,
          """
                          {
                              "Id": 1,
                              "value": "a"
                          }
                          """);
      final String DOC_B_JSON =
          """
                          {
                              "Id": 2,
                              "value": "b"
                          }
                              """;
      insertOneInTable(TABLE_NAME, DOC_B_JSON);

      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_NAME)
          .postFindOne(
              """
              {
                    "filter": {
                        "Id": 2
                    }
              }
          """)
          .hasNoErrors()
          .hasJSONField("data.document", DOC_B_JSON);
    }

    @Test
    @Order(3)
    public void findOneDocIdKey() {
      final String TABLE_NAME = "findOneDocIdKeyTable";
      createTableWithColumns(
          TABLE_NAME,
          Map.of(
              "_id",
              Map.of("type", "int"),
              "desc",
              Map.of("type", "text"),
              "valueLong",
              Map.of("type", "bigint"),
              "valueDouble",
              Map.of("type", "double")),
          "_id");

      // First, insert 2 documents:
      insertOneInTable(
          TABLE_NAME,
          """
                              {
                                  "_id": 1,
                                  "desc": "a",
                                  "valueLong": 1234567890,
                                  "valueDouble": -1.25
                              }
                              """);
      final String DOC_B_JSON =
          """
                              {
                                  "_id": 2,
                                  "desc": "b",
                                  "valueLong": 42,
                                  "valueDouble": 0.5
                              }
                                  """;
      insertOneInTable(TABLE_NAME, DOC_B_JSON);

      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_NAME)
          .postFindOne(
              """
                                          {
                                                "filter": {
                                                    "_id": 2
                                                }
                                          }
                                      """)
          .hasNoErrors()
          .hasJSONField("data.document", DOC_B_JSON);
    }
  }

  @Nested
  @Order(3)
  @TestClassOrder(ClassOrderer.OrderAnnotation.class)
  class FindOneTextColumns {
    public final String STRING_UTF8_WITH_2BYTE_CHAR = "utf8-2-byte-\u00a2"; // cent symbol
    public final String STRING_UTF8_WITH_3BYTE_CHAR = "utf8-3-byte-\u20ac"; // euro symbol

    @Test
    void insertWithTextColumnsAndFind() {
      final String DOC_JSON =
              """
                                  {
                                      "id": 1,
                                      "asciiText": "safe value",
                                      "varcharText": "%s/%s"
                                  }
                                  """
              .formatted(STRING_UTF8_WITH_2BYTE_CHAR, STRING_UTF8_WITH_3BYTE_CHAR);
      insertOneInTable(TABLE_WITH_TEXT_COLUMNS, DOC_JSON);

      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_WITH_TEXT_COLUMNS)
          .postFindOne("{ \"filter\": { \"id\": 1 } }")
          .hasNoErrors()
          .hasJSONField("data.document", DOC_JSON);
    }

    @Test
    void failTryingToInsertNonAscii() {
      final String DOC_JSON =
              """
                {
                    "id": 2,
                    "asciiText": "%s",
                    "varcharText": "safe value"
                }
                """
              .formatted(STRING_UTF8_WITH_2BYTE_CHAR);
      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_WITH_TEXT_COLUMNS)
          .postInsertOne(DOC_JSON)
          .hasSingleApiError(ErrorCodeV1.SERVER_INTERNAL_ERROR, "Invalid ASCII character");
    }
  }

  @Nested
  @Order(4)
  class FindOneFail {
    @Test
    @Order(1)
    public void failOnUnknownColumn() {
      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
                          {
                              "filter": {
                                "unknown": "a"
                              }
                          }
                      """)
          .hasNoData()
          .hasSingleApiError(
              ErrorCodeV1.TABLE_COLUMN_UNKNOWN,
              "Column unknown: No column with name 'unknown' found in table");
    }

    @Test
    @Order(2)
    public void failOnNonKeyColumn() {
      DataApiCommandSenders.assertTableCommand(namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
              {
                  "filter": {
                    "age": 80
                }
              }
          """)
          .hasNoData()
          // 22-Aug-2024, tatu: Not optimal, leftovers from Collections... but has to do
          .hasSingleApiError(ErrorCodeV1.NO_INDEX_ERROR, "Faulty collection (missing indexes).");
    }
  }
}
