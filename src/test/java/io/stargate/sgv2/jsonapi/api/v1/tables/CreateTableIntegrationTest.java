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
@WithTestResource(value = DseTestResource.class)
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
            .dropTable(testData.tableName(), false)
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
                  ErrorCodeV1.INVALID_REQUEST_STRUCTURE_MISMATCH.name(),
                  " may have a partitionSort field that is a JSON Object, each field is the name of a column, with a value of 1 for ASC, or -1 for DESC")));

      // invalidPartitionSortOrderingValueTypeTable
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
                  ErrorCodeV1.INVALID_REQUEST_STRUCTURE_MISMATCH.name(),
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
      // Column type not provided: nullColumnTypeTable
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
                  ErrorCodeV1.INVALID_REQUEST_STRUCTURE_MISMATCH.name(),
                  "The Long Form type definition must be a JSON Object with at least a `type` field that is a String (value is null)")));
      // unsupported primitive api types: timeuuid, counter
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                          {
                                           "name": "unsupportedPrimitiveApiTypes",
                                           "definition": {
                                               "columns": {
                                                   "id": {
                                                       "type": "text"
                                                   },
                                                   "timeuuid": {
                                                       "type": "timeuuid"
                                                   }
                                               },
                                               "primaryKey": "id"
                                           }
                                          }
                                          """,
                  "unsupportedPrimitiveApiTypes",
                  true,
                  SchemaException.Code.UNSUPPORTED_DATA_TYPE_TABLE_CREATION.name(),
                  "The command used the unsupported data types for table creation : timeuuid")));
      // Map type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                      {
                                        "name": "apiSupportedMap",
                                        "definition": {
                                          "columns": {
                                            "id": "text",
                                            "text2int": {
                                              "type": "map",
                                              "keyType": "text",
                                              "valueType": "int"
                                            },
                                           "int2text": {
                                              "type": "map",
                                              "keyType": "int",
                                              "valueType": "text"
                                            },
                                            "double2float": {
                                              "type": "map",
                                              "keyType": "double",
                                              "valueType": "float"
                                            },
                                            "blob2ascii": {
                                              "type": "map",
                                              "keyType": "blob",
                                              "valueType": "ascii"
                                            },
                                            "uuid2boolean": {
                                              "type": "map",
                                              "keyType": "uuid",
                                              "valueType": "boolean"
                                            },
                                            "decimal2duration": {
                                              "type": "map",
                                              "keyType": "decimal",
                                              "valueType": "duration"
                                            }
                                          },
                                          "primaryKey": "id"
                                        }
                                      }""",
                  "apiSupportedMap",
                  false,
                  null,
                  null)));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                      {
                                        "name": "unsupported",
                                        "definition": {
                                          "columns": {
                                            "id": "text",
                                            "age": "int",
                                            "name": "text",
                                            "map_type": {
                                              "type": "map",
                                              "keyType": "counter",
                                              "valueType": "text"
                                            }
                                          },
                                          "primaryKey": "id"
                                        }
                                      }""",
                  "unsupported map counter as key type",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the key type: counter.")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                      {
                                        "name": "unsupported",
                                        "definition": {
                                          "columns": {
                                            "id": "text",
                                            "age": "int",
                                            "name": "text",
                                            "map_type": {
                                              "type": "map",
                                              "keyType": "duration",
                                              "valueType": "text"
                                            }
                                          },
                                          "primaryKey": "id"
                                        }
                                      }""",
                  "unsupported map duration as key type",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the key type: duration.")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                      {
                                        "name": "unsupported",
                                        "definition": {
                                          "columns": {
                                            "id": "text",
                                            "age": "int",
                                            "name": "text",
                                            "map_type": {
                                              "type": "map",
                                              "keyType": "timeuuid",
                                              "valueType": "text"
                                            }
                                          },
                                          "primaryKey": "id"
                                        }
                                      }""",
                  "unsupported map timeuuid as key type",
                  true,
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.name(),
                  "The command used the key type: timeuuid.")));
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
                  "vectorizeConfigTest",
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

      // vector type with deprecated model
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                            {
                               "name": "deprecatedEmbedModel",
                               "definition": {
                                   "columns": {
                                       "id": {
                                           "type": "text"
                                       },
                                       "content": {
                                         "type": "vector",
                                         "dimension": 1024,
                                         "service": {
                                          "provider": "nvidia",
                                          "modelName": "a-deprecated-nvidia-embedding-model"
                                        }
                                       }
                                   },
                                   "primaryKey": "id"
                               }
                            }
                            """,
                  "deprecatedEmbedModel",
                  true,
                  SchemaException.Code.DEPRECATED_AI_MODEL.name(),
                  "The model is: a-deprecated-nvidia-embedding-model. It is at DEPRECATED status.")));

      // vector type with end_of_life model
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                        {
                                           "name": "deprecatedEmbedModel",
                                           "definition": {
                                               "columns": {
                                                   "id": {
                                                       "type": "text"
                                                   },
                                                   "content": {
                                                     "type": "vector",
                                                     "dimension": 1024,
                                                     "service": {
                                                      "provider": "nvidia",
                                                      "modelName": "a-EOL-nvidia-embedding-model"
                                                    }
                                                   }
                                               },
                                               "primaryKey": "id"
                                           }
                                        }
                                        """,
                  "deprecatedEmbedModel",
                  true,
                  SchemaException.Code.END_OF_LIFE_AI_MODEL.name(),
                  "The model is: a-EOL-nvidia-embedding-model. It is at END_OF_LIFE status.")));

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

      // unspecified dimension with specified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                        {
                            "name": "validUnspecifiedDimension",
                            "definition": {
                                "columns": {
                                    "t": "text",
                                    "v": {
                                        "type": "vector",
                                        "service": {
                                            "provider": "openai",
                                            "modelName": "text-embedding-3-small"
                                        }
                                    }
                                },
                                "primaryKey": "t"
                            }
                        }
                   """,
                  "validUnspecifiedDimension",
                  false,
                  null,
                  null)));

      // empty dimension string with specified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                        {
                            "name": "validEmptyDimension",
                            "definition": {
                                "columns": {
                                    "t": "text",
                                    "v": {
                                        "type": "vector",
                                        "dimension": "",
                                        "service": {
                                            "provider": "openai",
                                            "modelName": "text-embedding-3-small"
                                        }
                                    }
                                },
                                "primaryKey": "t"
                            }
                        }
                   """,
                  "validEmptyDimension",
                  false,
                  null,
                  null)));

      // blank dimension string with specified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                            {
                                "name": "validBlankDimension",
                                "definition": {
                                    "columns": {
                                        "t": "text",
                                        "v": {
                                            "type": "vector",
                                            "dimension": " ",
                                            "service": {
                                                "provider": "openai",
                                                "modelName": "text-embedding-3-small"
                                            }
                                        }
                                    },
                                    "primaryKey": "t"
                                }
                            }
                     """,
                  "validBlankDimension",
                  false,
                  null,
                  null)));

      // unspecified dimension with unspecified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                            {
                                "name": "invalidUnspecifiedDimensionUnspecifiedVectorize",
                                "definition": {
                                    "columns": {
                                        "t": "text",
                                        "v": {
                                            "type": "vector"
                                        }
                                    },
                                    "primaryKey": "t"
                                }
                            }
                     """,
                  "invalidUnspecifiedDimensionUnspecifiedVectorize",
                  true,
                  SchemaException.Code.MISSING_DIMENSION_IN_VECTOR_COLUMN.name(),
                  "The dimension is required for vector columns if the embedding service is not specified.")));

      // Two vector columns with the one has vectorizeDefinition and the other one doesn't.
      // This should be allowed.
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                  {
                                     "name": "twoVectorColumnsWithOneVectorizeDefinition",
                                     "definition": {
                                        "columns": {
                                            "t": "text",
                                            "v1": {
                                                "type": "vector",
                                                "dimension": "5",
                                                "service": {
                                                    "provider": "openai",
                                                    "modelName": "text-embedding-3-small"
                                                }
                                            },
                                            "v2":{
                                                "type": "vector",
                                                "dimension": "1024"
                                            }
                                        },
                                        "primaryKey": "t"
                                     }
                                  }
                                  """,
                  "twoVectorColumnsWithOneVectorizeDefinition",
                  false,
                  null,
                  null)));

      // table name is empty
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                        {
                            "name": "",
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
                  "",
                  true,
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name(),
                  "The command used the unsupported Table name: ''.")));

      // table name is black
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                        {
                            "name": " ",
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
                  " ",
                  true,
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name(),
                  "The command used the unsupported Table name: ' '.")));

      // table name too long
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                            {
                                "name": "this_is_a_very_long_table_name_that_is_longer_than_48_characters",
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
                  "this_is_a_very_long_table_name_that_is_longer_than_48_characters",
                  true,
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name(),
                  "The command used the unsupported Table name: 'this_is_a_very_long_table_name_that_is_longer_than_48_characters'.")));

      // table name with special characters
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                {
                                    "name": " !@#",
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
                  " !@#",
                  true,
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.name(),
                  "The command used the unsupported Table name: ' !@#'.")));

      return testCases.stream();
    }
  }
}
