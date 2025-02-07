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
                Map.entry("list_type_int_value", Map.of("type", "list", "valueType", "int")),
                Map.entry("set_type", Map.of("type", "set", "valueType", "text")),
                Map.entry("set_type_float_value", Map.of("type", "set", "valueType", "float")),
                Map.entry(
                    "map_type", Map.of("type", "map", "keyType", "text", "valueType", "text")),
                Map.entry(
                    "map_type_int_key",
                    Map.of("type", "map", "keyType", "int", "valueType", "text")),
                Map.entry(
                    "map_type_float_value",
                    Map.of("type", "map", "keyType", "text", "valueType", "float")),
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
                                      "column": "list_type"
                                    }
                                  }
                                  """)
          .wasSuccessful();

      verifyCreatedIndex("list_type_idx");
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropIndex("list_type_idx", false)
          .wasSuccessful();
    }

    @Test
    public void createSetIndex() {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
              """
                                  {
                                    "name": "set_type_idx",
                                    "definition": {
                                      "column": "set_type"
                                    }
                                  }
                                  """)
          .wasSuccessful();

      verifyCreatedIndex("set_type_idx");
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropIndex("set_type_idx", false)
          .wasSuccessful();
    }

    private static Stream<Arguments> listSetColumnsWithAnalyzerOptions() {
      return Stream.of(
          Arguments.of("list_type", true),
          Arguments.of("set_type", true),
          Arguments.of("list_type_int_value", false),
          Arguments.of("set_type_float_value", false));
    }

    /*
    set/list with value types that are text or ascii can have analyzer options
    */
    @ParameterizedTest
    @MethodSource("listSetColumnsWithAnalyzerOptions")
    public void listSetIndexWithAnalyzerOption(String listColumn, boolean valid) {
      if (valid) {
        assertTableCommand(keyspaceName, testTableName)
            .postCreateIndex(
                    """
                                {
                                  "name": "list_type_idx_analyzer_option",
                                  "definition": {
                                    "column": "%s",
                                    "options": {
                                        "caseSensitive": true,
                                        "normalize": true,
                                        "ascii": true
                                    }
                                  }
                                }
                                """
                    .formatted(listColumn))
            .wasSuccessful();
        verifyCreatedIndex("list_type_idx_analyzer_option");
        assertNamespaceCommand(keyspaceName)
            .templated()
            .dropIndex("list_type_idx_analyzer_option", false)
            .wasSuccessful();
      } else {
        assertTableCommand(keyspaceName, testTableName)
            .postCreateIndex(
                    """
                                {
                                  "name": "list_type_idx",
                                  "definition": {
                                    "column": "%s",
                                    "options": {
                                        "caseSensitive": true,
                                        "normalize": true,
                                        "ascii": true
                                    }
                                  }
                                }
                                """
                    .formatted(listColumn))
            .hasSingleApiError(
                SchemaException.Code.UNSUPPORTED_TEXT_ANALYSIS_FOR_DATA_TYPES,
                SchemaException.class,
                "column that uses a data type not supported for text analysis");
      }
    }

    private static Stream<Arguments> indexFunctionOnMap() {
      return Stream.of(
          // default to entries
          Arguments.of("\"map_type\""),
          Arguments.of("{\"map_type\": \"$keys\"}"),
          Arguments.of("{\"map_type\": \"$values\"}"));
    }

    @ParameterizedTest
    @MethodSource("indexFunctionOnMap")
    public void createMapIndex(String columnValue) {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
                  """
                                          {
                                            "name": "map_type_idx",
                                            "definition": {
                                              "column": %s
                                            }
                                          }
                                          """
                  .formatted(columnValue))
          .wasSuccessful();

      verifyCreatedIndex("map_type_idx");
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropIndex("map_type_idx", false)
          .wasSuccessful();
    }

    private static Stream<Arguments> mapColumnWithAnalyzerOptions() {
      return Stream.of(
          Arguments.of("map_type", "$keys", true),
          Arguments.of("map_type", "$values", true),
          Arguments.of("map_type_int_key", "$keys", false),
          Arguments.of("map_type_float_value", "$values", false));
    }

    /*
    set/list with value types that are text or ascii can have analyzer options
    */
    @ParameterizedTest
    @MethodSource("mapColumnWithAnalyzerOptions")
    public void mapIndexWithAnalyzerOption(String column, String indexFunction, boolean valid) {
      if (valid) {
        assertTableCommand(keyspaceName, testTableName)
            .postCreateIndex(
                    """
                                {
                                  "name": "map_type_idx_analyzer_option",
                                  "definition": {
                                    "column": {"%s":"%s"},
                                    "options": {
                                        "caseSensitive": true,
                                        "normalize": true,
                                        "ascii": true
                                    }
                                  }
                                }
                                """
                    .formatted(column, indexFunction))
            .wasSuccessful();
        verifyCreatedIndex("map_type_idx_analyzer_option");
        assertNamespaceCommand(keyspaceName)
            .templated()
            .dropIndex("map_type_idx_analyzer_option", false)
            .wasSuccessful();
      } else {
        assertTableCommand(keyspaceName, testTableName)
            .postCreateIndex(
                    """
                                {
                                  "name": "map_type_idx_analyzer_option",
                                  "definition": {
                                    "column": {"%s":"%s"},
                                    "options": {
                                        "caseSensitive": true,
                                        "normalize": true,
                                        "ascii": true
                                    }
                                  }
                                }
                                """
                    .formatted(column, indexFunction))
            .hasSingleApiError(
                SchemaException.Code.UNSUPPORTED_TEXT_ANALYSIS_FOR_DATA_TYPES,
                SchemaException.class,
                "column that uses a data type not supported for text analysis");
      }
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
              "The known index types are: regular, text, vector.",
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

    private static Stream<Arguments> invalidIndexFunction() {
      return Stream.of(
          Arguments.of("\"$keyss\""),
          Arguments.of("\"monkey\""),
          Arguments.of("\"keys\""),
          Arguments.of("\"values\""),
          Arguments.of("\"entries\""),
          Arguments.of(123));
    }

    @ParameterizedTest
    @MethodSource("invalidIndexFunction")
    public void invalidCommandForIndexFunction(Object indexFunction) {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
                  """
                                          {
                                            "name": "invalidIndexFunction",
                                            "definition": {
                                              "column": {"map_type" : %s},
                                              "options": {
                                                  "caseSensitive": true,
                                                  "normalize": true,
                                                  "ascii": true
                                              }
                                            }
                                          }
                                          """
                  .formatted(indexFunction))
          .hasSingleApiError(
              SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN,
              SchemaException.class,
              "Command has an invalid format for index creation column.");
    }

    private static Stream<Arguments> entriesIndexOnMap() {
      return Stream.of(Arguments.of("\"map_type\""));
    }

    @ParameterizedTest
    @MethodSource("entriesIndexOnMap")
    public void analyzeOptionsForEntriesIndexOnMap(String columnValue) {
      assertTableCommand(keyspaceName, testTableName)
          .postCreateIndex(
                  """
                                  {
                                    "name": "invalid",
                                    "definition": {
                                      "column": %s,
                                      "options": {
                                          "caseSensitive": true,
                                          "normalize": true,
                                          "ascii": true
                                      }
                                    }
                                  }
                                  """
                  .formatted(columnValue))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_ANALYZE_ENTRIES_ON_MAP_COLUMNS,
              SchemaException.class,
              "Index function `entries` can not apply to map column when analyze options are specified.");
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
              "The known index types are: regular, text, vector.",
              "The command used the unknown index type: unknown.");
    }
  }
}
