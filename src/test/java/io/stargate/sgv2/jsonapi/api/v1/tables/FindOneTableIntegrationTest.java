package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import jakarta.ws.rs.core.Response;
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
  static final String TABLE_WITH_PARTIAL_INDEXES = "findOneTableWithPartialIndexes";

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
  }

  // On-empty tests to be run before ones that populate tables
  @Nested
  @Order(1)
  class FindOneOnEmpty {
    @Test
    public void findOnEmptyNoFilter() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne("{ }")
          .hasNoErrors()
          .hasNoField("data.document");
    }

    @Test
    public void findOnEmptyNonMatchingFilter() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
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
      // First, insert 3 documents:
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

      // Third one with missing "age" and null "name"
      insertOneInTable(
          TABLE_WITH_STRING_ID_AGE_NAME,
          """
                              {
                                  "id": "c",
                                  "name": null
                              }
                              """);

      // First, find the second document:
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
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

      // And then third
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
                                  {
                                      "filter": {
                                          "id": "c"
                                      },
                                      "projection": {
                                            "id": 1, "age": 1, "name": 1
                                        }
                                  }
                              """)
          .hasNoErrors()
          .hasJSONField(
              "data.document",
              """
                              {
                                  "id": "c",
                                  "age": null,
                                  "name": null
                              }
                              """);
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

      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_NAME)
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
              Map.of("type", "double"),
              "valueBlob",
              Map.of("type", "blob")),
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
                                  "valueDouble": 0.5,
                                  "valueBlob": null
                              }
                              """;
      insertOneInTable(TABLE_NAME, DOC_B_JSON);

      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_NAME)
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
  class FindOneFail {
    @Test
    @Order(1)
    public void failOnUnknownColumn() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
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
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .expectHttpStatus(Response.Status.INTERNAL_SERVER_ERROR)
          .postFindOne(
              """
              {
                  "filter": {
                    "age": 80
                }
              }
          """)
      // 22-Aug-2024, tatu: Not optimal, leftovers from Collections... but has to do
      // 26 sept 2024, aaron: more not optimal, it is now a 500 UNEXPECTED_SERVER_ERROR and
      // ALLOW FILTERING until we get better
      ;
    }
  }

  @Nested
  @Order(4)
  // TODO all allow filtering read should have warning msg returned
  class FindOneWithAllowFiltering {

    final String doc_mary =
        """
                                  {
                                      "id": 1,
                                      "age": 30,
                                      "name": "Mary"
                                  }
                                  """;

    @Test
    @Order(1)
    public void createTableWithPartialIndexes() {
      createTableWithColumns(
          TABLE_WITH_PARTIAL_INDEXES,
          Map.of(
              "id",
              Map.of("type", "text"),
              "age",
              Map.of("type", "int"),
              "name",
              Map.of("type", "text")),
          "id");
      createIndex(TABLE_WITH_PARTIAL_INDEXES, "name", "nameIndexOnTableWithPartialIndexes");
      // Notice here, column age does not have an index on it.

      // insert rows for further tests:
      insertOneInTable(TABLE_WITH_PARTIAL_INDEXES, doc_mary);
    }

    @Test
    @Order(2)
    public void allowFilteringOnIndexMissingColumn() {
      // Notice here, column age does not have an index on it.
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_PARTIAL_INDEXES)
          .postFindOne(
              """
                      {
                          "filter": {
                            "age": 30
                        }
                      }
                  """)
          .hasNoErrors()
          .hasJSONField("data.document", doc_mary);
      // TODO hasWarning()
    }

    @Test
    @Order(2)
    public void noAllowFilteringOnIndexMissingColumn() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_PARTIAL_INDEXES)
          .postFindOne(
              """
                      {
                          "filter": {
                            "name": "Mary"
                        }
                      }
                  """)
          .hasNoErrors()
          .hasJSONField("data.document", doc_mary);
    }
  }
}
