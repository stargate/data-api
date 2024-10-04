package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        createTableErrorValidation(
            testData.request(), testData.errorCode(), testData.errorMessage());
      } else {
        createTable(testData.request());
        deleteTable(testData.tableName());
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
                                                                        "name": {
                                                                          "type": "text"
                                                                        }
                                                                      },
                                                                      "primaryKey": {
                                                                        "partitionBy": [
                                                                          "id"
                                                                        ],
                                                                        "partitionSort" : {
                                                                          "name" : 1, "age" : -1
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
      SchemaException missingDefinition =
          SchemaException.Code.COLUMN_DEFINITION_MISSING.get(Map.of("column_name", "error_column"));
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
                  missingDefinition.code,
                  missingDefinition.body)));

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
                  missingDefinition.code,
                  missingDefinition.body)));

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
                  missingDefinition.code,
                  missingDefinition.body)));

      SchemaException se = SchemaException.Code.PRIMARY_KEY_DEFINITION_INCORRECT.get();
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
                  se.code,
                  se.body)));

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
                  se.code,
                  se.body)));

      // invalidColumnTypeTable
      Map<String, String> errorMessageFormattingValues =
          Map.of(
              "type",
              "invalid_type",
              "supported_types",
              "[" + String.join(", ", ColumnType.getSupportedTypes()) + "]");
      SchemaException invalidType =
          SchemaException.Code.COLUMN_TYPE_UNSUPPORTED.get(errorMessageFormattingValues);
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
                  invalidType.code,
                  invalidType.body)));
      // Column type not provided
      SchemaException columnTypeInvalid =
          SchemaException.Code.COLUMN_TYPE_INCORRECT.get(errorMessageFormattingValues);
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                                  {
                                                                    "name": "invalidColumnTypeTable",
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
                  "invalidColumnTypeTable",
                  true,
                  columnTypeInvalid.code,
                  columnTypeInvalid.body)));

      // Map type tests
      SchemaException invalidMapType = SchemaException.Code.MAP_TYPE_INCORRECT_DEFINITION.get();
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidMapType",
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
                                                          }
                                                                              """,
                  "invalidMapType value type not provided",
                  true,
                  invalidMapType.code,
                  invalidMapType.body)));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidMapType",
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
                                                          }
                                                                              """,
                  "invalidMapType key type not provided",
                  true,
                  invalidMapType.code,
                  invalidMapType.body)));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidMapType",
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
                  "invalidMapType not primitive type provided",
                  true,
                  invalidMapType.code,
                  invalidMapType.body)));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidMapType",
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
                                                          }
                                                                              """,
                  "invalidMapType not primitive type provided",
                  true,
                  invalidMapType.code,
                  invalidMapType.body)));

      // List type tests
      SchemaException invalidListType = SchemaException.Code.LIST_TYPE_INCORRECT_DEFINITION.get();
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidListType",
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
                  "invalidListType value type not provided",
                  true,
                  invalidListType.code,
                  invalidListType.body)));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidListType",
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
                  "invalidListType not primitive type provided",
                  true,
                  invalidListType.code,
                  invalidListType.body)));

      // Set type tests
      SchemaException invalidSetType = SchemaException.Code.SET_TYPE_INCORRECT_DEFINITION.get();
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidSetType",
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
                                                          }
                                                                              """,
                  "invalidSetType value type not provided",
                  true,
                  invalidSetType.code,
                  invalidSetType.body)));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  """
                                                          {
                                                            "name": "invalidSetType",
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
                  "invalidSetType not primitive type provided",
                  true,
                  invalidSetType.code,
                  invalidSetType.body)));

      // Vector type tests
      SchemaException invalidVectorType =
          SchemaException.Code.VECTOR_TYPE_INCORRECT_DEFINITION.get();
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
                  invalidVectorType.code,
                  invalidVectorType.body)));

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
                  invalidVectorType.code,
                  invalidVectorType.body)));
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
                  false,
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
            false,
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
            false,
            ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS.name(),
            "The provided options are invalid: The provided dimension value '1536' doesn't match the model's supported dimension value '1024'")));
      return testCases.stream();
    }
  }
}
