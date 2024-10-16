package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateTableIndexIntegrationTest extends AbstractTableIntegrationTestBase {
  String testTableName = "tableForCreateIndexTest";

  @BeforeAll
  public final void createSimpleTable() {
    createTableWithColumns(
        testTableName,
        Map.ofEntries(
            Map.entry("id", Map.of("type", "text")),
            Map.entry("age", Map.of("type", "int")),
            Map.entry("comment", Map.of("type", "text")),
            Map.entry("vehicle_id", Map.of("type", "text")),
            Map.entry("vehicle_id_1", Map.of("type", "text")),
            Map.entry("vehicle_id_2", Map.of("type", "text")),
            Map.entry("vehicle_id_3", Map.of("type", "text")),
            Map.entry("vehicle_id_4", Map.of("type", "text")),
            Map.entry("invalid_text", Map.of("type", "int")),
            Map.entry("physicalAddress", Map.of("type", "text")),
            Map.entry("list_type", Map.of("type", "list", "valueType", "text")),
            Map.entry("set_type", Map.of("type", "set", "valueType", "text")),
            Map.entry("map_type", Map.of("type", "map", "keyType", "text", "valueType", "text")),
            Map.entry("vector_type_1", Map.of("type", "vector", "dimension", 1024)),
            Map.entry("vector_type_2", Map.of("type", "vector", "dimension", 1536)),
            Map.entry("vector_type_3", Map.of("type", "vector", "dimension", 1024))),
        "id");
  }

  @Nested
  @Order(1)
  class CreateIndexSuccess {

    @Test
    public void createIndexBasic() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                  {
                          "name": "age_idx",
                          "definition": {
                             "column": "age"
                          }
                  }
                  """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createIndexCaseSensitive() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                          {
                            "name": "vehicle_id_idx",
                            "definition": {
                                    "column": "vehicle_id"
                            }
                          }
                          """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createIndexCaseInsensitive() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
            {
              "name": "vehicle_id_1_idx",
              "definition": {
                "column": "vehicle_id_1",
                "options": {
                   "caseSensitive": false
                }
              }
            }
            """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createIndexConvertAscii() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                  {
                                    "name": "vehicle_id_2_idx",
                                    "definition": {
                                      "column": "vehicle_id_2",
                                      "options": {
                                         "ascii": true
                                      }
                                    }
                                  }
                                  """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createIndexNormalize() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                  {
                                    "name": "vehicle_id_3_idx",
                                    "definition": {
                                      "column": "vehicle_id_3",
                                      "options": {
                                        "normalize": true
                                      }
                                    }
                                  }
                                  """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createTextIndexAllOptions() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                      {
                        "name": "vehicle_id_4_idx",
                        "definition": {
                          "column": "vehicle_id_4",
                          "options": {
                            "caseSensitive": true,
                            "normalize": true,
                            "ascii": true
                          }
                        }
                      }
                              """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createListIndex() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                  {
                                    "name": "list_type_idx",
                                    "definition": {
                                      "column": "list_type"
                                    }
                                  }
                                  """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createSetIndex() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                  {
                                    "name": "set_type_idx",
                                    "definition": {
                                      "column": "set_type"
                                    }
                                  }
                                  """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createMapIndex() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                  {
                                    "name": "map_type_idx",
                                    "definition": {
                                      "column": "map_type"
                                    }
                                  }
                                  """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createIndexForQuotedColumn() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                          {
                                            "name": "physicalAddress_idx",
                                            "definition": {
                                              "column": "physicalAddress"
                                            }
                                          }
                                          """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createIndexForWithIfNotExist() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                {
                                  "name": "comment_idx",
                                  "definition": {
                                    "column": "comment"
                                  }
                                }
                                """)
          .hasNoErrors()
          .body("status.ok", is(1));

      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                                {
                                  "name": "comment_idx",
                                  "definition": {
                                    "column": "comment"
                                  },
                                  "options": {
                                    "ifNotExists": true
                                  }
                                }
                                """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class CreateVectorIndexSuccess {
    @Test
    public void createVectorIndex() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createVectorIndex",
              """
                                          {
                                            "name": "vector_type_1_idx",
                                            "definition": {
                                              "column": "vector_type_1"
                                            }
                                          }
                                          """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createVectorIndexWithSourceModel() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createVectorIndex",
              """
                                          {
                                            "name": "vector_type_2_idx",
                                            "definition": {
                                              "column": "vector_type_2",
                                              "options": {
                                                "sourceModel": "openai_v3_small"
                                              }
                                            }
                                          }
                                          """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createVectorIndexWithMetric() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createVectorIndex",
              """
                      {
                        "name": "vector_type_3_idx",
                        "definition": {
                          "column": "vector_type_3",
                          "options": {
                            "metric": "euclidean"
                          }
                        }
                      }
                      """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }

    @Test
    public void createVectorIndexWithMetricAndSourceModel() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createVectorIndex",
              """
                              {
                                "name": "vector_type_3_idx",
                                "definition": {
                                  "column": "vector_type_3",
                                  "options": {
                                    "metric": "cosine",
                                    "sourceModel": "mistral-embed"
                                  }
                                }
                              }
                              """)
          .hasNoErrors()
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(3)
  class CreateIndexFailure {
    @Test
    public void tryCreateIndexMissingColumn() {
      final SchemaException schemaException =
          SchemaException.Code.INVALID_INDEX_DEFINITION.get(
              Map.of("reason", "Column not defined in the table"));
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                      {
                        "name": "city_index",
                        "definition": {
                          "column": "city"
                        }
                      }
                      """)
          .hasSingleApiError(
              SchemaException.Code.INVALID_INDEX_DEFINITION,
              SchemaException.class,
              schemaException.body);
    }

    @Test
    public void nonTextOptions() {
      final SchemaException schemaException =
          SchemaException.Code.INVALID_INDEX_DEFINITION.get(
              Map.of(
                  "reason",
                  "`caseSensitive`, `normalize` and `ascii` options are valid only for `text` column"));
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createIndex",
              """
                              {
                                      "name": "invalid_text_idx",
                                      "definition": {
                                        "column": "invalid_text",
                                        "options": {
                                          "caseSensitive": true
                                        }
                                      }
                              }
                              """)
          .hasSingleApiError(
              SchemaException.Code.INVALID_INDEX_DEFINITION,
              SchemaException.class,
              schemaException.body);
    }
  }

  @Nested
  @Order(4)
  class CreateVectorIndexFailure {
    @Test
    public void tryCreateIndexMissingColumn() {
      final SchemaException schemaException =
          SchemaException.Code.INVALID_INDEX_DEFINITION.get(
              Map.of("reason", "Column not defined in the table"));
      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCommand(
              "createVectorIndex",
              """
                              {
                                "name": "city_index",
                                "definition": {
                                  "column": "city"
                                }
                              }
                              """)
          .hasSingleApiError(
              SchemaException.Code.INVALID_INDEX_DEFINITION,
              SchemaException.class,
              schemaException.body);
    }
  }
}
