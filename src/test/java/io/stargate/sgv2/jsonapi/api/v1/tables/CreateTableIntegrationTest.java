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
  private record CreateTableTestData(
      String tableName, String request, Enum<?> errorCode, String errorMessage) {
    // Constructor for passing (non-erroring) tests
    CreateTableTestData(String tableName, String request) {
      this(tableName, request, null, null);
    }

    public boolean hasErrorCode() {
      return errorCode != null;
    }
  }

  @Nested
  @Order(1)
  class CreateTable {
    @ParameterizedTest
    @MethodSource("allTableDataPass")
    public void testCreateTablePass(CreateTableTestData testData) {
      doTestCreateTable(testData);
    }

    @ParameterizedTest
    @MethodSource("allTableDataFail")
    public void testCreateTableFail(CreateTableTestData testData) {
      doTestCreateTable(testData);
    }

    private void doTestCreateTable(CreateTableTestData testData) {
      if (testData.hasErrorCode()) {
        assertNamespaceCommand(keyspaceName)
            .postCreateTable(testData.request())
            .hasSingleApiError(testData.errorCode().name(), testData.errorMessage());
      } else {
        assertNamespaceCommand(keyspaceName).postCreateTable(testData.request()).wasSuccessful();

        assertNamespaceCommand(keyspaceName)
            .templated()
            .dropTable(testData.tableName(), false)
            .wasSuccessful();
      }
    }

    // Set of passing ITs
    static Stream<Arguments> allTableDataPass() {
      List<Arguments> testCases = new ArrayList<>();
      // create table with all types
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "allTypesTable",
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
                                        """)));
      // primaryKeyAsString
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "primaryKeyAsStringTable",
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
                                        """)));

      // primaryKeyWithQuotable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "primaryKeyWithQuotable",
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
                                        """)));

      // columnTypeUsingShortHandTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "columnTypeUsingShortHandTable",
                  """
                                        {
                                           "name": "columnTypeUsingShortHandTable",
                                           "definition": {
                                               "columns": {
                                                   "id": "text",
                                                   "age": "int",
                                                   "name": "text"
                                               },
                                               "primaryKey": "id"
                                           }
                                        }
                                        """)));

      // primaryKeyAsJsonObjectTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "primaryKeyAsJsonObjectTable",
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
                                        """)));

      // Map type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "apiSupportedMap",
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
                                        }""")));

      // vector type with vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "vectorizeConfigTest",
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
                                        """)));

      // unspecified dimension with specified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "validUnspecifiedDimension",
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
                                        """)));

      // empty dimension string with specified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "validEmptyDimension",
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
                                        """)));

      // blank dimension string with specified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "validBlankDimension",
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
                                        """)));

      // Two vector columns with the one has vectorizeDefinition and the other one doesn't.
      // This should be allowed.
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "twoVectorColumnsWithOneVectorizeDefinition",
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
                                        """)));

      return testCases.stream();
    }

    static Stream<Arguments> allTableDataFail() {
      List<Arguments> testCases = new ArrayList<>();

      // invalidPrimaryKeyTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidPrimaryKeyTable",
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
                  SchemaException.Code.UNKNOWN_PARTITION_COLUMNS,
                  "The partition includes the unknown columns: error_column.")));
      // invalidPartitionByTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidPartitionByTable",
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
                  SchemaException.Code.UNKNOWN_PARTITION_COLUMNS,
                  "The partition includes the unknown columns: error_column.")));
      // invalidPartitionSortTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidPartitionSortTable",
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
                  SchemaException.Code.UNKNOWN_PARTITION_SORT_COLUMNS,
                  "The partition sort includes the unknown columns: error_column.")));
      // invalidPartitionSortOrderingValueTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidPartitionSortOrderingValueTable",
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
                  ErrorCodeV1.INVALID_REQUEST_STRUCTURE_MISMATCH,
                  " may have a partitionSort field that is a JSON Object, each field is the name of a column, with a value of 1 for ASC, or -1 for DESC")));
      // invalidPartitionSortOrderingValueTypeTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidPartitionSortOrderingValueTypeTable",
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
                  ErrorCodeV1.INVALID_REQUEST_STRUCTURE_MISMATCH,
                  " may have a partitionSort field that is a JSON Object, each field is the name of a column, with a value of 1 for ASC, or -1 for DESC")));
      // invalidColumnTypeTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidColumnTypeTable",
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
                  SchemaException.Code.UNKNOWN_PRIMITIVE_DATA_TYPE,
                  "The command used the unsupported data type: invalid_type.")));
      // Column type not provided: nullColumnTypeTable
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "nullColumnTypeTable",
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
                  ErrorCodeV1.INVALID_REQUEST_STRUCTURE_MISMATCH,
                  "The Long Form type definition must be a JSON Object with at least a `type` field that is a String (value is null)")));
      // unsupported primitive api types: timeuuid, counter
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "unsupportedPrimitiveApiTypes",
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
                  SchemaException.Code.UNSUPPORTED_DATA_TYPE_TABLE_CREATION,
                  "The command used the unsupported data types for table creation : timeuuid")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "unsupportedMapCounterAsKeyType",
                  """
                                                      {
                                                        "name": "unsupportedMapCounterAsKeyType",
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
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
                  "The command used the key type: counter.")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "unsupportedMapDurationAsKeyType",
                  """
                                                      {
                                                        "name": "unsupportedMapDurationAsKeyType",
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
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
                  "The command used the key type: duration.")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "unsupportedMapUuidAsKeyType",
                  """
                                                      {
                                                        "name": "unsupportedMapUuidAsKeyType",
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
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
                  "The command used the key type: timeuuid.")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "mapTypeMissingValue",
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
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
                  "The command used the value type: [MISSING].")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "mapTypeMissingKeyType",
                  """
                                                      {
                                                        "name": "mapTypeMissingKeyType",
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
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
                  "The command used the key type: [MISSING].")));
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "mapTypeListValueType",
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
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
                  "The command used the value type: list.")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "mapTypeListKeyType",
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
                  SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
                  "The command used the value type: text.")));

      // List type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "listTypeMissingValueType",
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
                  SchemaException.Code.UNSUPPORTED_LIST_DEFINITION,
                  "The command used the value type: [MISSING].")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "listTypeListValueType",
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
                  SchemaException.Code.UNSUPPORTED_LIST_DEFINITION,
                  "The command used the value type: list.")));

      // Set type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "listTypeMissingValueType",
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
                  SchemaException.Code.UNSUPPORTED_SET_DEFINITION,
                  "The command used the value type: [MISSING].")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "listTypeListValueType",
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
                  SchemaException.Code.UNSUPPORTED_SET_DEFINITION,
                  "The command used the value type: list.")));

      // Vector type tests
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidVectorDimensionNegative",
                  """
                          {
                            "name": "invalidVectorDimensionNegative",
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
                  SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION,
                  "The command used the dimension: -5.")));

      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidVectorDimensionNotNumber",
                  """
                          {
                            "name": "invalidVectorDimensionNotNumber",
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
                  SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION,
                  "The command used the dimension: aaa.")));

      // vector type with invalid vectorize config
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidVectorizeServiceNameConfig",
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
                  ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS,
                  "The provided options are invalid: Service provider 'invalid_service' is not supported")));

      // vector type with invalid model name config
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidVectorizeModelNameConfig",
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
                  ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS,
                  "The provided options are invalid: Model name 'mistral-embed-invalid' for provider 'mistral' is not supported")));

      // vector type with deprecated model
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "deprecatedEmbedModel",
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
                  SchemaException.Code.DEPRECATED_AI_MODEL,
                  "The model is: a-deprecated-nvidia-embedding-model. It is at DEPRECATED status.")));

      // vector type with end_of_life model
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "eolEmbedModel",
                  """
                                            {
                                               "name": "eolEmbedModel",
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
                  SchemaException.Code.END_OF_LIFE_AI_MODEL,
                  "The model is: a-EOL-nvidia-embedding-model. It is at END_OF_LIFE status.")));

      // vector type with dimension mismatch
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidVectorizeModelNameConfig",
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
                  ErrorCodeV1.INVALID_CREATE_COLLECTION_OPTIONS,
                  "The provided options are invalid: The provided dimension value '1536' doesn't match the model's supported dimension value '1024'")));

      // unspecified dimension with unspecified vectorize
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "invalidUnspecifiedDimensionUnspecifiedVectorize",
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
                  SchemaException.Code.MISSING_DIMENSION_IN_VECTOR_COLUMN,
                  "The dimension is required for vector columns if the embedding service is not specified.")));

      // table name is empty
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "",
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
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
                  "The command used the unsupported Table name: ''.")));

      // table name is blank
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  " ",
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
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
                  "The command used the unsupported Table name: ' '.")));

      // table name too long
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  "this_is_a_very_long_table_name_that_is_longer_than_48_characters",
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
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
                  "The command used the unsupported Table name: 'this_is_a_very_long_table_name_that_is_longer_than_48_characters'.")));

      // table name with special characters
      testCases.add(
          Arguments.of(
              new CreateTableTestData(
                  " !@#",
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
                  SchemaException.Code.UNSUPPORTED_SCHEMA_NAME,
                  "The command used the unsupported Table name: ' !@#'.")));
      return testCases.stream();
    }
  }
}
