package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@QuarkusIntegrationTest
@QuarkusTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneTableIntegrationTest extends AbstractTableIntegrationTestBase {
  static final String TABLE_WITH_STRING_ID_AGE_NAME = "findOneSingleStringKeyTable";

  @BeforeAll
  public final void createDefaultTables() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            TABLE_WITH_STRING_ID_AGE_NAME,
            Map.of(
                "id",
                Map.of("type", "text"),
                "age",
                Map.of("type", "int"),
                "name",
                Map.of("type", "text")),
            "id")
        .wasSuccessful();
  }

  // On-empty tests to be run before ones that populate tables
  @Nested
  @Order(1)
  class FindOneOnEmpty {
    @Test
    public void findOnEmptyNoFilter() {
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne("{ }")
          .wasSuccessful()
          .hasNoField("data.document");
    }

    @Test
    public void findOnEmptyNonMatchingFilter() {
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
                                  {
                                    "filter": {
                                        "id": "nosuchkey"
                                    }
                                  }
                              """)
          .wasSuccessful()
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
      var docJSON =
          """
                      {
                          "id": "a",
                          "age": 20,
                          "name": "John"
                      }
                      """;
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      final String DOC_B_JSON =
          """
                          {
                              "id": "b",
                              "age": 40,
                              "name": "Bob"
                          }
                          """;
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .insertOne(DOC_B_JSON)
          .wasSuccessful();

      // Third one with missing "age" and null "name"
      docJSON =
          """
              {
                "id": "c",
                "name": null
              }""";
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .templated()
          .insertOne(docJSON)
          .wasSuccessful();

      // First, find the second document:
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
                          {
                              "filter": {
                                  "id": "b"
                              }
                          }
                      """)
          .wasSuccessful()
          .hasJSONField("data.document", DOC_B_JSON);

      // And then third
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
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
          .wasSuccessful()
          .hasJSONField(
              "data.document",
              // By default, null values are not returned
              removeNullValues(
                  """
                                  {
                                      "id": "c",
                                      "age": null,
                                      "name": null
                                  }
                                  """));
    }

    @Test
    @Order(2)
    public void findOneDocUpperCaseKey() {
      final String TABLE_NAME = "findOneDocUpperCaseKey";
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              TABLE_NAME,
              Map.of("Id", Map.of("type", "int"), "value", Map.of("type", "text")),
              "Id")
          .wasSuccessful();

      // Insert 2 documents:
      var docJSON =
          """
                          {
                              "Id": 1,
                              "value": "a"
                          }
                          """;
      assertTableCommand(keyspaceName, TABLE_NAME).templated().insertOne(docJSON).wasSuccessful();

      final String DOC_B_JSON =
          """
                          {
                              "Id": 2,
                              "value": "b"
                          }
                          """;
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .insertOne(DOC_B_JSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_NAME)
          .postFindOne(
              """
              {
                    "filter": {
                        "Id": 2
                    }
              }
          """)
          .wasSuccessful()
          .hasJSONField("data.document", DOC_B_JSON);
    }

    @Test
    @Order(3)
    public void findOneDocIdKey() {
      final String TABLE_NAME = "findOneDocIdKeyTable";
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
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
              "_id")
          .wasSuccessful();

      // First, insert 2 documents:
      var docJSON =
          """
                              {
                                  "_id": 1,
                                  "desc": "a",
                                  "valueLong": 1234567890,
                                  "valueDouble": -1.25
                              }
                              """;
      assertTableCommand(keyspaceName, TABLE_NAME).templated().insertOne(docJSON).wasSuccessful();

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
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .insertOne(DOC_B_JSON)
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_NAME)
          .postFindOne(
              """
              {
                    "filter": {
                        "_id": 2
                    }
              }
              """)
          .wasSuccessful()
          .hasJSONField("data.document", removeNullValues(DOC_B_JSON));
    }

    // for [data-api#1532]
    @Test
    @Order(4)
    void documentIdWith$in() {
      final String TABLE_NAME = "findOneIdAndDollarInTable";
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              TABLE_NAME,
              Map.of(
                  "_id", "text",
                  "value", "int"),
              "_id")
          .wasSuccessful();

      // First, insert 2 documents:
      String DOC_A_JSON = "{ \"_id\": \"a\", \"value\": 12 }";
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .insertOne(DOC_A_JSON)
          .wasSuccessful();

      String DOC_B_JSON = "{ \"_id\": \"b\", \"value\": 23 }";
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .insertOne(DOC_B_JSON)
          .wasSuccessful();

      // First find one of documents by column other than _id
      assertTableCommand(keyspaceName, TABLE_NAME)
          .postFindOne(
              """
                                      {
                                            "filter": {
                                                "value": { "$in": [12] }
                                            }
                                      }
                                      """)
          .wasSuccessful()
          .hasJSONField("data.document", DOC_A_JSON);

      // Then try to find one of documents by _id
      assertTableCommand(keyspaceName, TABLE_NAME)
          .postFindOne(
              """
                              {
                                    "filter": {
                                        "_id": { "$in": ["b", "c"] }
                                    }
                              }
                              """)
          .wasSuccessful()
          .hasJSONField("data.document", DOC_B_JSON);
    }

    @Test
    @Order(5)
    public final void sparseDataForCollectionDataType() {
      String tableName = "mapListSet";
      String tableJson =
              """
                          {
                              "name": "%s",
                              "definition": {
                                  "columns": {
                                      "id": "text",
                                      "map_type": {
                                          "type": "map",
                                          "keyType": "text",
                                          "valueType": "text"
                                      },
                                      "list_type": {
                                          "type": "list",
                                          "valueType": "text"
                                      },
                                      "set_type": {
                                          "type": "set",
                                          "valueType": "text"
                                      }
                                  },
                                  "primaryKey": "id"
                              }
                          }
                          """
              .formatted(tableName);
      assertNamespaceCommand(keyspaceName).postCreateTable(tableJson).wasSuccessful();
      // insert 1 document:
      var docJSON =
          """
               {
                   "id": "1"
               }
               """;
      assertTableCommand(keyspaceName, tableName).templated().insertOne(docJSON).wasSuccessful();
      // find the document, verify null set/list/map are not returned
      assertTableCommand(keyspaceName, tableName)
          .templated()
          .findOne(ImmutableMap.of("id", "1"), null)
          .wasSuccessful()
          .hasJSONField("data.document", docJSON);
    }
  }

  @Nested
  @Order(3)
  class FindOneFail {
    @Test
    @Order(1)
    public void failOnUnknownColumn() {
      assertTableCommand(keyspaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .postFindOne(
              """
                          {
                              "filter": {
                                "unknown": "a"
                              }
                          }
                      """)
          .hasSingleApiError(
              FilterException.Code.UNKNOWN_TABLE_COLUMNS,
              FilterException.class,
              "Only columns defined in the table schema can be filtered");
    }
  }
}
