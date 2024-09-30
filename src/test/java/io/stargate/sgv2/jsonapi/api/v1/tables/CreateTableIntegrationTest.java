package io.stargate.sgv2.jsonapi.api.v1.tables;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
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
      return testCases.stream();
    }
  }
}
