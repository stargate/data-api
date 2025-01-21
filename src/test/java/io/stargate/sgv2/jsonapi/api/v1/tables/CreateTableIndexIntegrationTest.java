package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateTableIndexIntegrationTest extends AbstractTableIntegrationTestBase {
  String testTableName = "tableForCreateIndexTest";

  private void verifyCreatedIndex(String indexName) {
    assertTableCommand(keyspaceName, testTableName)
        .templated()
        .listIndexes(false)
        .wasSuccessful()
        .hasIndex(indexName);
  }

  @BeforeAll
  public final void createSimpleTable() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
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
                Map.entry("vehicle_id_5", Map.of("type", "text")),
                Map.entry("invalid_text", Map.of("type", "int")),
                Map.entry("physicalAddress", Map.of("type", "text")),
                Map.entry("list_type", Map.of("type", "list", "valueType", "text")),
                Map.entry("set_type", Map.of("type", "set", "valueType", "text")),
                Map.entry(
                    "map_type", Map.of("type", "map", "keyType", "text", "valueType", "text")),
                Map.entry("vector_type_1", Map.of("type", "vector", "dimension", 1024)),
                Map.entry("vector_type_2", Map.of("type", "vector", "dimension", 1536)),
                Map.entry("vector_type_3", Map.of("type", "vector", "dimension", 1024)),
                Map.entry("vector_type_4", Map.of("type", "vector", "dimension", 1024)),
                Map.entry("vector_type_5", Map.of("type", "vector", "dimension", 1024)),
                Map.entry("vector_type_6", Map.of("type", "vector", "dimension", 1024)),
                Map.entry("vector_type_7", Map.of("type", "vector", "dimension", 1024))),
            "id")
        .wasSuccessful();
  }

  @Nested
  @Order(1)
  class CreateIndexSuccess {

    @Test
    public void createIndexBasic() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                  {
                          "name": "age_idx",
                          "definition": {
                             "column": "age"
                          }
                  }
                  """)
          .wasSuccessful();

      verifyCreatedIndex("age_idx");
    }

    @Test
    public void createIndexCaseSensitive() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                          {
                            "name": "vehicle_id_idx",
                            "definition": {
                                    "column": "vehicle_id"
                            }
                          }
                          """)
          .wasSuccessful();

      verifyCreatedIndex("vehicle_id_idx");
    }

    @Test
    public void createIndexCaseInsensitive() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
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
          .wasSuccessful();

      verifyCreatedIndex("vehicle_id_1_idx");
    }

    @Test
    public void createIndexConvertAscii() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
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
          .wasSuccessful();

      verifyCreatedIndex("vehicle_id_2_idx");
    }

    @Test
    public void createIndexNormalize() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
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
          .wasSuccessful();

      verifyCreatedIndex("vehicle_id_3_idx");
    }

    @Test
    public void createTextIndexAllOptions() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
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
          .wasSuccessful();

      verifyCreatedIndex("vehicle_id_4_idx");
    }

    @Test
    public void createListIndex() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                  {
                                    "name": "list_type_idx",
                                    "definition": {
                                      "column": "list_type",
                                      "indexFunction": "values"
                                    }
                                  }
                                  """)
          .wasSuccessful();

      verifyCreatedIndex("list_type_idx");
    }

    @Test
    public void createSetIndex() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                  {
                                    "name": "set_type_idx",
                                    "definition": {
                                      "column": "set_type",
                                      "indexFunction": "values"
                                    }
                                  }
                                  """)
          .wasSuccessful();

      verifyCreatedIndex("set_type_idx");
    }

    @Test
    public void createMapIndex() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                  {
                                    "name": "map_type_idx_on_keys",
                                    "definition": {
                                      "column": "map_type",
                                      "indexFunction": "keys"
                                    }
                                  }
                                  """)
          .wasSuccessful();

      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                          {
                                            "name": "map_type_idx_on_values",
                                            "definition": {
                                              "column": "map_type",
                                              "indexFunction": "values"
                                            }
                                          }
                                          """)
          .wasSuccessful();

      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                          {
                                            "name": "map_type_idx_on_entries",
                                            "definition": {
                                              "column": "map_type",
                                              "indexFunction": "entries"
                                            }
                                          }
                                          """)
          .wasSuccessful();

      verifyCreatedIndex("map_type_idx_on_keys");
      verifyCreatedIndex("map_type_idx_on_values");
      verifyCreatedIndex("map_type_idx_on_entries");
    }

    @Test
    public void createIndexForQuotedColumn() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                          {
                                            "name": "physicalAddress_idx",
                                            "definition": {
                                              "column": "physicalAddress"
                                            }
                                          }
                                          """)
          .wasSuccessful();

      verifyCreatedIndex("physicalAddress_idx");
    }

    @Test
    public void createIndexForWithIfNotExist() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                {
                                  "name": "comment_idx",
                                  "definition": {
                                    "column": "comment"
                                  }
                                }
                                """)
          .wasSuccessful();

      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
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
          .wasSuccessful();

      verifyCreatedIndex("comment_idx");
    }

    @Test
    public void createIndexWithCorrectIndexType() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                {
                                  "name": "vehicle_id_5_idx",
                                  "definition": {
                                    "column": "vehicle_id_5"
                                  },
                                  "indexType": "regular"
                                }
                                """)
          .wasSuccessful();

      verifyCreatedIndex("vehicle_id_5_idx");
    }
  }

  @Nested
  @Order(2)
  class CreateVectorIndexSuccess {
    @Test
    public void createVectorIndex() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                                          {
                                            "name": "vector_type_1_idx",
                                            "definition": {
                                              "column": "vector_type_1"
                                            }
                                          }
                                          """)
          .wasSuccessful();

      verifyCreatedIndex("vector_type_1_idx");
    }

    @Test
    public void createVectorIndexWithSourceModel() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                                          {
                                            "name": "vector_type_2_idx",
                                            "definition": {
                                              "column": "vector_type_2",
                                              "options": {
                                                "sourceModel": "openai-v3-small"
                                              }
                                            }
                                          }
                                          """)
          .wasSuccessful();

      verifyCreatedIndex("vector_type_2_idx");
    }

    @Test
    public void createVectorIndexWithMetric() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
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
          .wasSuccessful();

      verifyCreatedIndex("vector_type_3_idx");
    }

    @Test
    public void createVectorIndexWithMetricAndSourceModel() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                              {
                                "name": "vector_type_4_idx",
                                "definition": {
                                  "column": "vector_type_4",
                                  "options": {
                                    "metric": "cosine",
                                    "sourceModel": "openai-v3-small"
                                  }
                                }
                              }
                              """)
          .wasSuccessful();

      verifyCreatedIndex("vector_type_4_idx");
    }

    @Test
    public void createVectorIndexWithCorrectIndexType() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                                {
                                  "name": "vector_type_6_idx",
                                  "definition": {
                                    "column": "vector_type_6"
                                  },
                                  "indexType": "vector"
                                }
                                """)
          .wasSuccessful();

      verifyCreatedIndex("vector_type_6_idx");
    }
  }

  @Nested
  @Order(3)
  class CreateIndexFailure {
    @Test
    public void tryCreateIndexMissingColumn() {

      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                      {
                        "name": "city_index",
                        "definition": {
                          "column": "city"
                        }
                      }
                      """)
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_INDEX_COLUMN,
              SchemaException.class,
              "The command attempted to index the unknown columns: city.");
    }

    @Test
    public void nonTextOptions() {

      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                              {
                                      "name": "invalid_text_idx",
                                      "definition": {
                                        "column": "invalid_text",
                                        "options": {
                                          "normalize": true
                                        }
                                      }
                              }
                              """)
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_TEXT_ANALYSIS_FOR_DATA_TYPES,
              SchemaException.class,
              "The command attempted to index the unsupported columns: invalid_text(int).");
    }

    @Test
    public void createIndexWithUnsupportedIndexType() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                              {
                                "name": "vehicle_id_7_idx",
                                "definition": {
                                  "column": "vehicle_id_7"
                                },
                                "indexType": "vector"
                              }
                              """)
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_INDEX_TYPE,
              SchemaException.class,
              "The supported index types are: regular.",
              "The command used the unsupported index type: vector.");
    }

    @Test
    public void createIndexWithUnknownIndexType() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                      {
                                        "name": "vehicle_id_7_idx",
                                        "definition": {
                                          "column": "vehicle_id_7"
                                        },
                                        "indexType": "unknown"
                                      }
                                      """)
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_INDEX_TYPE,
              SchemaException.class,
              "The known index types are: [collection, regular, text-analysed, vector].",
              "The command used the unknown index type: unknown.");
    }

    // [data-api#1812]: invalid JSON structure
    @Test
    public void invalidJSONStructure() {
      assertTableCommand(keyspaceName, testTableName)
          // Invalid JSON structure: "name" should be String, not Object;
          // reported as HTTP 400
          .expectHttpStatus(Response.Status.BAD_REQUEST)
          .postCreateIndex(
              """
                        {
                            "name": {
                              "name": 1
                            },
                            "definition": {
                              "column": {
                                "background": true
                              },
                            }
                        }
                        """)
          .hasSingleApiError(
              ErrorCodeV1.INVALID_REQUEST_STRUCTURE_MISMATCH,
              "Request invalid, mismatching JSON structure: underlying problem");
    }

    private static Stream<Arguments> unmatchedIndexFunctionWithDataType() {
      var commands = new ArrayList<Arguments>();
      commands.add(Arguments.of("list_type", "keys"));
      commands.add(Arguments.of("list_type", "entries"));
      commands.add(Arguments.of("set_type", "keys"));
      commands.add(Arguments.of("set_type", "entries"));
      return commands.stream();
    }

    @ParameterizedTest
    @MethodSource("unmatchedIndexFunctionWithDataType")
    public void createKeyEntriesIndexOnListSet(String column, String indexFunction) {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
                  """
                                  {
                                    "name": "unmatchedIndexFunction",
                                    "definition": {
                                      "column": "%s",
                                      "indexFunction": "%s"
                                    }
                                  }
                                  """
                  .formatted(column, indexFunction))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_APPLY_INDEX_FUNCTION_KEYS_ENTRIES_TO_NON_MAP_COLUMN,
              SchemaException.class,
              "Index function `keys`, `entries` can not apply to columns other than map.");
    }

    private static Stream<Arguments> collectionColumns() {
      var commands = new ArrayList<Arguments>();
      commands.add(Arguments.of("list_type"));
      commands.add(Arguments.of("set_type"));
      commands.add(Arguments.of("map_type"));
      return commands.stream();
    }

    @ParameterizedTest
    @MethodSource("collectionColumns")
    public void createIndexWithoutIndexFunction(String column) {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
                  """
                                  {
                                    "name": "missIndexFunction",
                                    "definition": {
                                      "column": "%s"
                                    }
                                  }
                                  """
                  .formatted(column))
          .hasSingleApiError(
              SchemaException.Code.MISSING_INDEX_FUNCTION_FOR_COLLECTION_COLUMN,
              SchemaException.class,
              "The index function is required for map,set,list columns when creating the index.");
    }

    @Test
    public void cannotAnalyzeOnMapColumn() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                  {
                                    "name": "canNotAnalyzeOnMapColumn",
                                    "definition": {
                                      "column": "map_type",
                                      "indexFunction": "entries",
                                      "options": {
                                          "caseSensitive": true,
                                          "normalize": true,
                                          "ascii": true
                                      }
                                    }
                                  }
                                  """)
          .hasSingleApiError(
              SchemaException.Code.CANNOT_ANALYZE_ENTRIES_ON_MAP_COLUMNS,
              SchemaException.class,
              "Index function `entries` can not apply to map column when analyze options are specified.");
    }

    @ParameterizedTest
    @MethodSource("collectionColumns")
    public void unsupportedIndexFunction(String column) {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
                  """
                                  {
                                    "name": "unsupportedIndexFunction",
                                    "definition": {
                                      "column": "%s",
                                      "indexFunction": "full"
                                    }
                                  }
                                  """
                  .formatted(column))
          .hasSingleApiError(
              ErrorCodeV1.COMMAND_FIELD_INVALID,
              "support index functions are keys/values/entries.");
    }
  }

  @Nested
  @Order(4)
  class CreateVectorIndexFailure {
    @Test
    public void tryCreateIndexMissingColumn() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                              {
                                "name": "city_index",
                                "definition": {
                                  "column": "city"
                                }
                              }
                              """)
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_INDEX_COLUMN,
              SchemaException.class,
              "The command attempted to index the unknown columns: city.");
    }

    @Test
    public void invalidSourceModel() {

      DataApiCommandSenders.assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                {
                  "name": "vector_type_5_idx",
                  "definition": {
                    "column": "vector_type_5",
                    "options": {
                      "sourceModel": "invalid_source_model"
                    }
                  }
                }
                """)
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_VECTOR_SOURCE_MODEL,
              SchemaException.class,
              "The command attempted to use the source model: invalid_source_model.");
    }

    @Test
    public void createVectorIndexWithUnsupportedIndexType() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                              {
                                "name": "vector_type_7_idx",
                                "definition": {
                                  "column": "vector_type_7"
                                },
                                "indexType": "regular"
                              }
                              """)
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_INDEX_TYPE,
              SchemaException.class,
              "The supported index types are: vector.",
              "The command used the unsupported index type: regular.");
    }

    @Test
    public void createVectorIndexWithUnknownIndexType() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateVectorIndex(
              """
                                {
                                    "name": "vector_type_7_idx",
                                    "definition": {
                                    "column": "vector_type_7"
                                    },
                                    "indexType": "unknown"
                                }
                                """)
          .hasSingleApiError(
              SchemaException.Code.UNKNOWN_INDEX_TYPE,
              SchemaException.class,
              "The known index types are: [collection, regular, text-analysed, vector].",
              "The command used the unknown index type: unknown.");
    }
  }
}
