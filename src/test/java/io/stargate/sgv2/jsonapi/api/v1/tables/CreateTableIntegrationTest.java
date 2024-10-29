package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateTableIntegrationTest extends AbstractTableIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateTable {
    @ParameterizedTest
    @MethodSource("allTableData")
    public void testCreateTable(CreateTableTestData testData) {
      if (testData.error()) {
        assertNamespaceCommand(keyspaceName)
            .postCreateTable(testData.request())
            .hasSingleApiError(testData.errorCode(), testData.errorMessage());
      } else {
        assertNamespaceCommand(keyspaceName).postCreateTable(testData.request()).wasSuccessful();

        assertNamespaceCommand(keyspaceName)
            .templated()
            .dropTable(testData.tableName())
            .wasSuccessful();
      }
    }

    private record CreateTableTestData(
        String request, String tableName, boolean error, String errorCode, String errorMessage) {}

    private static Stream<Arguments> allTableData() {
      List<Arguments> testCases = new ArrayList<>();
      // create table with all types
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                    {
                                                       "name": "allTypesTable",
                                                       "definition": {
                                                           "columns": {
                                                               "ascii_type": "ascii",
                                                               "bigint_type": "bigint",
                                                               "blob_type": "blob",
                                                               "boolean_type": "boolean",
                                                               "date_type": "date",
                                                               "decimal_type": "decimal",
                                                               "double_type": "double",
                                                               "duration_type": "duration",
                                                               "float_type": "float",
                                                               "inet_type": "inet",
                                                               "int_type": "int",
                                                               "smallint_type": "smallint",
                                                               "text_type": "text",
                                                               "time_type": "time",
                                                               "timestamp_type": "timestamp",
                                                               "tinyint_type": "tinyint",
                                                               "uuid_type": "uuid",
                                                               "varint_type": "varint",
                                                               "map_type": {
                                                                  "type": "map",
                                                                  "keyType": "text",
                                                                  "valueType": "int"
                                                               },
                                                               "list_type": {
                                                                  "type": "list",
                                                                  "valueType": "text"
                                                               },
                                                               "set_type": {
                                                                  "type": "set",
                                                                  "valueType": "text"
                                                               },
                                                               "vector_type": {
                                                                  "type": "vector",
                                                                  "dimension": 5
                                                               }
                                                           },
                                                           "primaryKey": "text_type"
                                                       }
                                                    }
                                                    """,
                  "allTypesTable",
                  false,
                  null,
                  null)));
      // primaryKeyAsString
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                                  {
                                                                     "name": "primaryKeyAsStringTable",
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
                                                                  """,
                  "primaryKeyAsStringTable",
                  false,
                  null,
                  null)));
      // primaryKeyAsString
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                      {
                                                         "name": "primaryKeyAsStringTable",
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
                                                      """,
                  "primaryKeyAsStringTable",
                  false,
                  null,
                  null)));

      // primaryKeyWithQuotable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                      {
                                                            "name": "primaryKeyWithQuotable",
                                                            "definition": {
                                                              "primaryKey": "_id",
                                                              "columns": {
                                                                "_id": {
                                                                  "type": "text"
                                                                },
                                                                "name": {
                                                                  "type": "text"
                                                                }
                                                              }
                                                            }
                                                        }
                                                      """,
                  "primaryKeyWithQuotable",
                  false,
                  null,
                  null)));

      // columnTypeusingShortHandTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                      {
                                                         "name": "columnTypeusingShortHandTable",
                                                         "definition": {
                                                             "columns": {
                                                                 "id": "text",
                                                                 "age": "int",
                                                                 "name": "text"
                                                             },
                                                             "primaryKey": "id"
                                                         }
                                                      }
                                                      """,
                  "columnTypeusingShortHandTable",
                  false,
                  null,
                  null)));

      // primaryKeyAsJsonObjectTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                                  {
                                                                    "name": "primaryKeyAsJsonObjectTable",
                                                                    "definition": {
                                                                      "columns": {
                                                                        "id": {
                                                                          "type": "text"
                                                                        },
                                                                        "age": {
                                                                          "type": "int"
                                                                        },
                                                                        "fullName": {
                                                                          "type": "text"
                                                                        }
                                                                      },
                                                                      "primaryKey": {
                                                                        "partitionBy": [
                                                                          "id"
                                                                        ],
                                                                        "partitionSort" : {
                                                                          "fullName" : 1, "age" : -1
                                                                        }
                                                                      }
                                                                    }
                                                                  }
                                                                  """,
                  "primaryKeyAsJsonObjectTable",
                  false,
                  null,
                  null)));

      // invalidPrimaryKeyTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                      {
                                                       "name": "invalidPrimaryKeyTable",
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
                                                           "primaryKey": "error_column"
                                                       }
                                                      }
                                                      """,
                  "invalidPrimaryKeyTable",
                  true,
                  SchemaException.Code.UNKNOWN_PARTITION_COLUMNS.name(),
                  "The partition includes the unknown columns: error_column.")));

      // invalidPartitionByTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                      {
                                                        "name": "invalidPartitionByTable",
                                                        "definition": {
                                                          "columns": {
                                                            "id": "text",
                                                            "age": "int",
                                                            "name": "text"
                                                          },
                                                          "primaryKey": {
                                                            "partitionBy": [
                                                              "error_column"
                                                            ],
                                                            "partitionSort" : {
                                                              "name" : 1, "age" : -1
                                                            }
                                                          }
                                                        }
                                                      }
                                                      """,
                  "invalidPartitionByTable",
                  true,
                  SchemaException.Code.UNKNOWN_PARTITION_COLUMNS.name(),
                  "The partition includes the unknown columns: error_column.")));

      // invalidPartitionSortTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                      {
                                                          "name": "invalidPartitionSortTable",
                                                          "definition": {
                                                            "columns": {
                                                              "id": "text",
                                                              "age": "int",
                                                              "name": "text"
                                                            },
                                                            "primaryKey": {
                                                              "partitionBy": [
                                                                "id"
                                                              ],
                                                              "partitionSort" : {
                                                                "error_column" : 1, "age" : -1
                                                              }
                                                            }
                                                          }
                                                        }
                                                      """,
                  "invalidPartitionSortTable",
                  true,
                  SchemaException.Code.UNKNOWN_PARTITION_SORT_COLUMNS.name(),
                  "The partition sort includes the unknown columns: error_column.")));

      // invalidPartitionSortOrderingValueTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "invalidPartitionSortOrderingValueTable",
                            "definition": {
                              "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text"
                              },
                              "primaryKey": {
                                "partitionBy": [
                                  "id"
                                ],
                                "partitionSort" : {
                                  "id" : 1, "age" : 0
                                }
                              }
                            }
                          }
                          """,
                  "invalidPartitionSortOrderingValueTable",
                  true,
                  ErrorCodeV1.INVALID_REQUEST_NOT_JSON.name(),
                  " may have a partitionSort field that is a JSON Object, each field is the name of a column, with a value of 1 for ASC, or -1 for DESC")));

      // invalidPartitionSortOrderingValueTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                              "name": "invalidPartitionSortOrderingValueTypeTable",
                              "definition": {
                                "columns": {
                                  "id": "text",
                                  "age": "int",
                                  "name": "text"
                                },
                                "primaryKey": {
                                  "partitionBy": [
                                    "id"
                                  ],
                                  "partitionSort" : {
                                    "id" : 1, "age" : "invalid"
                                  }
                                }
                              }
                            }
                          """,
                  "invalidPartitionSortOrderingValueTypeTable",
                  true,
                  ErrorCodeV1.INVALID_REQUEST_NOT_JSON.name(),
                  " may have a partitionSort field that is a JSON Object, each field is the name of a column, with a value of 1 for ASC, or -1 for DESC")));

      // invalidColumnTypeTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "invalidColumnTypeTable",
                            "definition": {
                              "columns": {
                                "id": "invalid_type",
                                "age": "int",
                                "name": "text"
                              },
                              "primaryKey": {
                                "partitionBy": [
                                    "id"
                                ],
                                "partitionSort": {
                                    "id": 1,
                                    "age": -1
                                }
                              }
                            }
                          }
                          """,
                  "invalidColumnTypeTable",
                  true,
                  SchemaException.Code.UNKNOWN_PRIMITIVE_DATA_TYPE.name(),
                  "The command used the unsupported data type: invalid_type.")));
      // Column type not provided
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "nullColumnTypeTable",
                            "definition": {
                              "columns": {
                                "id": null,
                                "age": "int",
                                "name": "text"
                              },
                              "primaryKey": {
                                "partitionBy": [
                                    "id"
                                ],
                                "partitionSort": {
                                    "id": 1,
                                    "age": -1
                                }
                              }
                            }
                          }
                          """,
                  "nullColumnTypeTable",
                  true,
                  ErrorCodeV1.INVALID_REQUEST_NOT_JSON.name(),
                  "The Long Form type definition must be a JSON Object with at least a `type` field that is a String (value is null)")));

      // Map type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "mapTypeMissingValue",
                            "definition": {
                              "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text",
                                "map_type": {
                                  "type": "map",
                                  "keyType": "text"
                                }
                              },
                              "primaryKey": "id"
                            }
                          }""",
                  "mapTypeMissingValue value type not provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the value type: [MISSING].")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "mapTypeMissingKey",
                            "definition": {
                              "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text",
                                "map_type": {
                                  "type": "map",
                                  "valueType": "text"
                                }
                              },
                              "primaryKey": "id"
                            }
                          }""",
                  "mapTypeMissingKey key type not provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the key type: [MISSING].")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "mapTypeListValueType",
                            "definition": {
                              "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text",
                                "map_type": {
                                  "type": "map",
                                  "valueType": "list",
                                  "keyType": "text"
                                }
                              },
                              "primaryKey": "id"
                            }
                          }
                          """,
                  "mapTypeListValueType not primitive type provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the value type: list.")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "mapTypeListKeyType",
                            "definition": {
                              "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text",
                                "map_type": {
                                  "type": "map",
                                  "valueType": "text",
                                  "keyType": "list"
                                }
                              },
                              "primaryKey": "id"
                            }
                          }""",
                  "mapTypeListKeyType not primitive type provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the value type: text.")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                              {
                                "name": "mapTypeNonStringKeyType",
                                "definition": {
                                  "columns": {
                                    "id": "text",
                                    "age": "int",
                                    "name": "text",
                                    "map_type": {
                                      "type": "map",
                                      "valueType": "text",
                                      "keyType": "int"
                                    }
                                  },
                                  "primaryKey": "id"
                                }
                              }
                              """,
                  "mapTypeNonStringKeyType not primitive type provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the key type: int.")));

      // List type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                            {
                              "name": "listTypeMissingValueType",
                              "definition": {
                                "columns": {
                                  "id": "text",
                                  "age": "int",
                                  "name": "text",
                                  "list_type": {
                                    "type": "list"
                                  }
                                },
                                "primaryKey": "id"
                              }
                            }
                            """,
                  "listTypeMissingValueType value type not provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_LIST_DEFINITION.name(),
                  "The command used the value type: [MISSING].")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                              {
                                "name": "listTypeListValueType",
                                "definition": {
                                  "columns": {
                                    "id": "text",
                                    "age": "int",
                                    "name": "text",
                                    "list_type": {
                                      "type": "list",
                                      "valueType": "list"
                                    }
                                  },
                                  "primaryKey": "id"
                                }
                              }
                              """,
                  "listTypeListValueType not primitive type provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_LIST_DEFINITION.name(),
                  "The command used the value type: list.")));

      // Set type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "listTypeMissingValueType",
                            "definition": {
                              "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text",
                                "set_type": {
                                  "type": "set"
                                }
                              },
                              "primaryKey": "id"
                            }
                          }""",
                  "listTypeMissingValueType value type not provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_SET_DEFINITION.name(),
                  "The command used the value type: [MISSING].")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                            {
                              "name": "listTypeListValueType",
                              "definition": {
                                "columns": {
                                  "id": "text",
                                  "age": "int",
                                  "name": "text",
                                  "set_type": {
                                    "type": "set",
                                    "valueType": "list"
                                  }
                                },
                                "primaryKey": "id"
                              }
                            }
                            """,
                  "listTypeListValueType not primitive type provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_SET_DEFINITION.name(),
                  "The command used the value type: list.")));

      // Vector type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                            {
                              "name": "invalidVectorType",
                              "definition": {
                                "columns": {
                                  "id": "text",
                                  "age": "int",
                                  "name": "text",
                                  "vector_type": {
                                    "type": "vector",
                                    "dimension": -5
                                  }
                                },
                                "primaryKey": "id"
                              }
                            }
                            """,
                  "invalidVectorType value type not provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION.name(),
                  "The command used the dimension: -5.")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                          {
                            "name": "invalidVectorType",
                            "definition": {
                              "columns": {
                                "id": "text",
                                "age": "int",
                                "name": "text",
                                "vector_type": {
                                  "type": "vector",
                                  "dimension": "aaa"
                                }
                              },
                              "primaryKey": "id"
                            }
                          }
                          """,
                  "invalidVectorType not primitive type provided",
                  true,
                  SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION.name(),
                  "The command used the dimension: aaa.")));
      // vector type with vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                    {
                                       "name": "vectorizeConfigTest",
                                       "definition": {
                                           "columns": {
                                               "id": {
                                                   "type": "text"
                                               },
                                               "age": {
                                                   "type": "int"
                                               },
                                               "content": {
                                                 "type": "vector",
                                                 "dimension": 1024,
                                                 "service": {
                                                   "provider": "nvidia",
                                                   "modelName": "NV-Embed-QA"
                                                 }
                                               }
                                           },
                                           "primaryKey": "id"
                                       }
                                    }
                                    """,
                  "primaryKeyAsStringTable",
                  false,
                  null,
                  null)));

      // vector type with invalid vectorixe config
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                  {
                     "name": "invalidVectorizeServiceNameConfig",
                     "definition": {
                         "columns": {
                             "id": {
                                 "type": "text"
                             },
                             "age": {
                                 "type": "int"
                             },
                             "content": {
                               "type": "vector",
                               "dimension": 1024,
                               "service": {
                                 "provider": "invalid_service",
                                 "modelName": "NV-Embed-QA"
                               }
                             }
                         },
                         "primaryKey": "id"
                     }
                  }
                  """,
                  "invalidVectorizeServiceNameConfig",
                  true,
                  ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.name(),
                  "The provided options are invalid: Service provider 'invalid_service' is not supported")));

      // vector type with invalid model name config
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
            {
               "name": "invalidVectorizeModelNameConfig",
               "definition": {
                   "columns": {
                       "id": {
                           "type": "text"
                       },
                       "age": {
                           "type": "int"
                       },
                       "content": {
                         "type": "vector",
                         "dimension": 1024,
                         "service": {
                          "provider": "mistral",
                          "modelName": "mistral-embed-invalid"
                        }
                       }
                   },
                   "primaryKey": "id"
               }
            }
            """,
                  "invalidVectorizeModelNameConfig",
                  true,
                  ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.name(),
                  "The provided options are invalid: Model name 'mistral-embed-invalid' for provider 'mistral' is not supported")));

      // vector type with dimension mismatch
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
            {
               "name": "invalidVectorizeModelNameConfig",
               "definition": {
                   "columns": {
                       "id": {
                           "type": "text"
                       },
                       "age": {
                           "type": "int"
                       },
                       "content": {
                         "type": "vector",
                         "dimension": 1536,
                         "service": {
                          "provider": "mistral",
                          "modelName": "mistral-embed"
                        }
                       }
                   },
                   "primaryKey": "id"
               }
            }
            """,
                  "invalidVectorizeModelNameConfig",
                  true,
                  ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.name(),
                  "The provided options are invalid: The provided dimension value '1536' doesn't match the model's supported dimension value '1024'")));
      return testCases.stream();
    }
  }
}
